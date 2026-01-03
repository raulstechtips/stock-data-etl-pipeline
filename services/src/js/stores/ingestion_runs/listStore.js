/**
 * Runs List Store - Alpine.js Store
 * Manages state and API interactions for the ingestion runs listing page
 * Handles filtering, pagination, and runs list display
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineRunsListStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Runs list store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('runsList', {
        // STATE
        _initialized: false,
        runs: [],
        loading: false,
        error: null,
        
        // Pagination
        nextCursor: null,
        previousCursor: null,
        pageSize: 50,
        
        // Debounce timers for real-time filtering
        _debounceTimers: {
            ticker__icontains: null,
            requested_by__icontains: null
        },
        
        // Debounce delay (ms) - configurable
        debounceDelay: 400,
        
        // Current API request controller for cancellation
        _currentRequestController: null,
        
        // Filters - removed exact filters (ticker, requested_by), kept for backward compatibility
        filters: {
            run_id: '',
            ticker__icontains: '',
            state: '',
            requested_by__icontains: '',
            created_after: '',
            created_before: '',
            is_terminal: '',
            is_in_progress: '',
            bulk_queue_run: '' // Kept in store but not displayed in UI
        },

        /**
         * Initialize the store (idempotent)
         * @param {object} initialFilters - Optional initial filters to apply
         */
        init(initialFilters = null) {
            // Apply initial filters if provided (even if already initialized)
            if (initialFilters && typeof initialFilters === 'object') {
                this.setInitialFilters(initialFilters);
            }
            
            // Guard: only initialize once
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Set initial filters from external source (e.g., URL parameters)
         * @param {object} initialFilters - Object with filter key-value pairs
         */
        setInitialFilters(initialFilters) {
            if (!initialFilters || typeof initialFilters !== 'object') {
                return;
            }
            
            // Apply filters that match the store's filter structure
            Object.keys(this.filters).forEach(key => {
                if (initialFilters.hasOwnProperty(key) && initialFilters[key] !== null && initialFilters[key] !== undefined) {
                    // Convert to string and trim (handles both strings and other types)
                    this.filters[key] = String(initialFilters[key]).trim();
                }
            });
        },

        /**
         * Load runs with current filters and pagination
         * @param {string} cursor - Optional pagination cursor (null for first page)
         */
        async loadRuns(cursor = null) {
            try {
                // Cancel any pending request
                if (this._currentRequestController) {
                    this._currentRequestController.abort();
                }
                
                // Create new AbortController for this request
                this._currentRequestController = new AbortController();
                const signal = this._currentRequestController.signal;
                
                this.loading = true;
                this.error = null;

                // Build filters object (only include non-empty values)
                const activeFilters = {};
                Object.keys(this.filters).forEach(key => {
                    const value = this.filters[key];
                    // Handle boolean filters - allow string "true"/"false" or boolean values
                    if (key === 'is_terminal' || key === 'is_in_progress') {
                        if (value !== null && value !== undefined && value !== '') {
                            // Convert string "true"/"false" to boolean, or keep boolean as is
                            if (typeof value === 'string') {
                                activeFilters[key] = value.toLowerCase() === 'true';
                            } else {
                                activeFilters[key] = Boolean(value);
                            }
                        }
                    } else if (key === 'created_after' || key === 'created_before') {
                        // Handle date filters - convert date picker (YYYY-MM-DD) to UTC ISO 8601
                        if (value && value.trim() !== '') {
                            // Date picker format is "YYYY-MM-DD" (user's local timezone)
                            // Convert local midnight to UTC ISO 8601 format
                            const dateValue = value.trim();
                            const utcISO = window.dateUtils.dateToUTCISO(dateValue);
                            if (utcISO) {
                                activeFilters[key] = utcISO;
                            }
                        }
                    } else {
                        // Handle string filters
                        if (value && value.trim() !== '') {
                            activeFilters[key] = value.trim();
                        }
                    }
                });

                // Get runs API store
                const runsAPI = Alpine.store('runsAPI');
                if (!runsAPI) {
                    throw new Error('Runs API store is not available');
                }

                // Call API (note: API client doesn't support AbortSignal yet, but we track it for future use)
                const response = await runsAPI.listRuns(this.pageSize, cursor, activeFilters);

                // Check if request was aborted (shouldn't happen with current API client, but check for safety)
                if (signal.aborted) {
                    this.loading = false;
                    return;
                }

                // Update state
                this.runs = response.results || [];
                this.nextCursor = response.next ? this.extractCursor(response.next) : null;
                this.previousCursor = response.previous ? this.extractCursor(response.previous) : null;

            } catch (error) {
                // Ignore abort errors
                if (error.name === 'AbortError' || (this._currentRequestController && this._currentRequestController.signal.aborted)) {
                    this.loading = false;
                    return;
                }
                this.error = error.message || 'Failed to load runs';
                console.error('Failed to load runs:', error);
                this.runs = [];
                this.nextCursor = null;
                this.previousCursor = null;
            } finally {
                // Clear loading state if this is still the current request and not aborted
                if (this._currentRequestController && !this._currentRequestController.signal.aborted) {
                    this.loading = false;
                }
            }
        },

        /**
         * Extract cursor from pagination URL
         * @param {string} url - Pagination URL from API response
         * @returns {string|null} - Cursor value or null
         */
        extractCursor(url) {
            try {
                const urlObj = new URL(url);
                return urlObj.searchParams.get('cursor');
            } catch (error) {
                console.error('Failed to extract cursor from URL:', error);
                return null;
            }
        },

        /**
         * Apply filters and reload runs (resets to first page)
         */
        async applyFilters() {
            // Reset pagination when filters change
            this.nextCursor = null;
            this.previousCursor = null;
            await this.loadRuns(null);
        },

        /**
         * Clear all filters and reload runs
         */
        async clearFilters() {
            // Clear debounce timers
            Object.keys(this._debounceTimers).forEach(key => {
                if (this._debounceTimers[key]) {
                    clearTimeout(this._debounceTimers[key]);
                    this._debounceTimers[key] = null;
                }
            });
            
            this.filters = {
                run_id: '',
                ticker__icontains: '',
                state: '',
                requested_by__icontains: '',
                created_after: '',
                created_before: '',
                is_terminal: '',
                is_in_progress: '',
                bulk_queue_run: ''
            };
            await this.applyFilters();
        },

        /**
         * Go to next page
         */
        async nextPage() {
            if (this.nextCursor) {
                await this.loadRuns(this.nextCursor);
            }
        },

        /**
         * Go to previous page
         */
        async previousPage() {
            if (this.previousCursor) {
                await this.loadRuns(this.previousCursor);
            }
        },

        /**
         * Check if there are active filters
         * @returns {boolean} - True if any filter has a value
         */
        hasActiveFilters() {
            return Object.values(this.filters).some(value => value && value.trim() !== '');
        },

        /**
         * Get count of active filters
         * @returns {number} - Number of active filters
         */
        getActiveFilterCount() {
            return Object.values(this.filters).filter(value => value && value.trim() !== '').length;
        },

        /**
         * Handle debounced filter changes for real-time filtering
         * @param {string} filterKey - Filter key (e.g., 'ticker__icontains', 'requested_by__icontains')
         * @param {string} value - New filter value
         */
        handleDebouncedFilter(filterKey, value) {
            // Clear existing timer for this filter
            if (this._debounceTimers[filterKey]) {
                clearTimeout(this._debounceTimers[filterKey]);
                this._debounceTimers[filterKey] = null;
            }

            // Update filter value immediately (for UI reactivity)
            this.filters[filterKey] = value;

            // Trim value for checking
            const trimmedValue = value.trim();

            // If empty, clear immediately without API call
            if (trimmedValue === '') {
                // Reset pagination and reload
                this.nextCursor = null;
                this.previousCursor = null;
                this.loadRuns(null);
                return;
            }

            // Set debounce timer
            this._debounceTimers[filterKey] = setTimeout(() => {
                // Reset pagination to first page when debounced filter triggers
                this.nextCursor = null;
                this.previousCursor = null;
                // Load runs with updated filter
                this.loadRuns(null);
                // Clear timer
                this._debounceTimers[filterKey] = null;
            }, this.debounceDelay);
        }
    });
}

// Timing-safe initialization
if (window.Alpine) {
    defineRunsListStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineRunsListStore();
    });
}

