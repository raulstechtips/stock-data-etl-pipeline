/**
 * Stocks List Store - Alpine.js Store
 * Manages state and API interactions for the stocks listing page
 * Handles filtering, pagination, and stock list display
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineStocksListStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Stocks list store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('stocksList', {
        // STATE
        _initialized: false,
        stocks: [],
        loading: false,
        error: null,
        
        // Pagination
        nextCursor: null,
        previousCursor: null,
        pageSize: 50,
        
        // Debounce timers for real-time filtering
        _debounceTimers: {
            ticker__icontains: null,
            sector__name__icontains: null
        },
        
        // Debounce delay (ms) - configurable
        debounceDelay: 400,
        
        // Current API request controller for cancellation
        _currentRequestController: null,
        
        // Exchanges list for dropdown
        exchanges: [],
        exchangesLoading: false,
        exchangesLoaded: false,
        
        // Filters - removed exact filters (ticker, sector__name), kept for backward compatibility
        filters: {
            ticker__icontains: '',
            sector__name__icontains: '',
            exchange__name: '',
            country: '' // Kept in store but not displayed in UI
        },

        /**
         * Initialize the store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Load stocks with current filters and pagination
         * @param {string} cursor - Optional pagination cursor (null for first page)
         */
        async loadStocks(cursor = null) {
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
                    if (value && value.trim() !== '') {
                        activeFilters[key] = value.trim();
                    }
                });

                // Get stocks API store
                const stocksAPI = Alpine.store('stocksAPI');
                if (!stocksAPI) {
                    throw new Error('Stocks API store is not available');
                }

                // Call API (note: API client doesn't support AbortSignal yet, but we track it for future use)
                const response = await stocksAPI.listTickers(this.pageSize, cursor, activeFilters);

                // Check if request was aborted (shouldn't happen with current API client, but check for safety)
                if (signal.aborted) {
                    this.loading = false;
                    return;
                }

                // Update state
                this.stocks = response.results || [];
                this.nextCursor = response.next ? this.extractCursor(response.next) : null;
                this.previousCursor = response.previous ? this.extractCursor(response.previous) : null;

            } catch (error) {
                // Ignore abort errors
                if (error.name === 'AbortError' || (this._currentRequestController && this._currentRequestController.signal.aborted)) {
                    this.loading = false;
                    return;
                }
                this.error = error.message || 'Failed to load stocks';
                console.error('Failed to load stocks:', error);
                this.stocks = [];
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
         * Apply filters and reload stocks (resets to first page)
         */
        async applyFilters() {
            // Reset pagination when filters change
            this.nextCursor = null;
            this.previousCursor = null;
            await this.loadStocks(null);
        },

        /**
         * Clear all filters and reload stocks
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
                ticker__icontains: '',
                sector__name__icontains: '',
                exchange__name: '',
                country: ''
            };
            await this.applyFilters();
        },

        /**
         * Go to next page
         */
        async nextPage() {
            if (this.nextCursor) {
                await this.loadStocks(this.nextCursor);
            }
        },

        /**
         * Go to previous page
         */
        async previousPage() {
            if (this.previousCursor) {
                await this.loadStocks(this.previousCursor);
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
         * @param {string} filterKey - Filter key (e.g., 'ticker__icontains', 'sector__name__icontains')
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
                this.loadStocks(null).catch(err => console.error('Failed to reload stocks:', err));
                return;
            }

            // Set debounce timer
            this._debounceTimers[filterKey] = setTimeout(() => {
                // Reset pagination to first page when debounced filter triggers
                this.nextCursor = null;
                this.previousCursor = null;
                // Load stocks with updated filter
                this.loadStocks(null).catch(err => console.error('Failed to reload stocks:', err));
                // Clear timer
                this._debounceTimers[filterKey] = null;
            }, this.debounceDelay);
        },

        /**
         * Load exchanges list from API (lazy loading - only on first call)
         * Caches results in store after first load
         */
        async loadExchanges() {
            // Return cached exchanges if already loaded
            if (this.exchangesLoaded) {
                return;
            }

            try {
                this.exchangesLoading = true;
                this.error = null;

                // Get metadata API store
                const metadataAPI = Alpine.store('metadataAPI');
                if (!metadataAPI) {
                    throw new Error('Metadata API store is not available');
                }

                // Fetch all exchanges (use large page size to get all at once)
                const response = await metadataAPI.listExchanges(100, null, {});

                // Store exchanges in state
                this.exchanges = response.results || [];
                this.exchangesLoaded = true;

            } catch (error) {
                this.error = error.message || 'Failed to load exchanges';
                console.error('Failed to load exchanges:', error);
                this.exchanges = [];
            } finally {
                this.exchangesLoading = false;
            }
        }
    });
}

// Timing-safe initialization
if (window.Alpine) {
    defineStocksListStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineStocksListStore();
    });
}

