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
        
        // Filters
        filters: {
            ticker: '',
            ticker__icontains: '',
            sector__name: '',
            sector__name__icontains: '',
            exchange__name: '',
            country: ''
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

                // Call API
                const response = await stocksAPI.listTickers(this.pageSize, cursor, activeFilters);

                // Update state
                this.stocks = response.results || [];
                this.nextCursor = response.next ? this.extractCursor(response.next) : null;
                this.previousCursor = response.previous ? this.extractCursor(response.previous) : null;

            } catch (error) {
                this.error = error.message || 'Failed to load stocks';
                console.error('Failed to load stocks:', error);
                this.stocks = [];
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
            this.filters = {
                ticker: '',
                ticker__icontains: '',
                sector__name: '',
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

