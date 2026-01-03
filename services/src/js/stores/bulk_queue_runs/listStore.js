/**
 * Bulk Queue Runs List Store - Alpine.js Store
 * Manages state and API interactions for the bulk queue runs listing page
 * Handles filtering, pagination, and bulk queue runs list display
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineBulkQueueRunsListStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Bulk queue runs list store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('bulkQueueRunsList', {
        // STATE
        _initialized: false,
        bulkQueueRuns: [],
        loading: false,
        error: null,
        
        // Pagination
        nextCursor: null,
        previousCursor: null,
        pageSize: 50,
        
        // Filters - all filter fields from API Documentation.md lines 936-949
        filters: {
            requested_by__icontains: '',
            created_after: '',
            created_before: '',
            started_at_after: '',
            started_at_before: '',
            completed_at_after: '',
            completed_at_before: '',
            is_completed: '',
            has_errors: ''
        },

        /**
         * Initialize the store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Load bulk queue runs with current filters and pagination
         * @param {string} cursor - Optional pagination cursor (null for first page)
         */
        async loadBulkQueueRuns(cursor = null) {
            try {
                this.loading = true;
                this.error = null;

                // Build filters object (only include non-empty values)
                const activeFilters = {};
                Object.keys(this.filters).forEach(key => {
                    const value = this.filters[key];
                    // Handle boolean filters - allow string "true"/"false" or boolean values
                    if (key === 'is_completed' || key === 'has_errors') {
                        if (value !== null && value !== undefined && value !== '') {
                            // Convert string "true"/"false" to boolean, or keep boolean as is
                            if (typeof value === 'string') {
                                activeFilters[key] = value.toLowerCase() === 'true';
                            } else {
                                activeFilters[key] = Boolean(value);
                            }
                        }
                    } else if (key === 'created_after' || key === 'created_before' || 
                               key === 'started_at_after' || key === 'started_at_before' ||
                               key === 'completed_at_after' || key === 'completed_at_before') {
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

                // Call API
                const response = await runsAPI.listBulkQueueRuns(this.pageSize, cursor, activeFilters);

                // Update state
                this.bulkQueueRuns = response.results || [];
                this.nextCursor = response.next ? this.extractCursor(response.next) : null;
                this.previousCursor = response.previous ? this.extractCursor(response.previous) : null;

            } catch (error) {
                this.error = error.message || 'Failed to load bulk queue runs';
                console.error('Failed to load bulk queue runs:', error);
                this.bulkQueueRuns = [];
                this.nextCursor = null;
                this.previousCursor = null;
            } finally {
                this.loading = false;
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
         * Apply filters and reload bulk queue runs (resets to first page)
         */
        async applyFilters() {
            // Reset pagination when filters change
            this.nextCursor = null;
            this.previousCursor = null;
            await this.loadBulkQueueRuns(null);
        },

        /**
         * Clear all filters and reload bulk queue runs
         */
        async clearFilters() {
            this.filters = {
                requested_by__icontains: '',
                created_after: '',
                created_before: '',
                started_at_after: '',
                started_at_before: '',
                completed_at_after: '',
                completed_at_before: '',
                is_completed: '',
                has_errors: ''
            };
            await this.applyFilters();
        },

        /**
         * Go to next page
         */
        async nextPage() {
            if (this.nextCursor) {
                await this.loadBulkQueueRuns(this.nextCursor);
            }
        },

        /**
         * Go to previous page
         */
        async previousPage() {
            if (this.previousCursor) {
                await this.loadBulkQueueRuns(this.previousCursor);
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
        }
    });
}

// Timing-safe initialization
if (window.Alpine) {
    defineBulkQueueRunsListStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineBulkQueueRunsListStore();
    });
}

