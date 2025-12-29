/**
 * Stocks API Store
 * Alpine.js store for stock ticker API endpoints
 * Handles stock listing, details, and status retrieval
 */

/**
 * Define the stocks API store
 */
function defineStocksAPI() {
    Alpine.store('stocksAPI', {
        // State
        loading: false,
        error: null,
        _initialized: false,

        /**
         * Initialize the stocks API store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * List all stocks with pagination and optional filters
         * @param {number} pageSize - Number of items per page (max: 100)
         * @param {string} cursor - Pagination cursor from previous response
         * @param {object} filters - Optional filter parameters
         * @param {string} filters.ticker - Exact ticker match (case-insensitive)
         * @param {string} filters.ticker__icontains - Ticker contains substring (case-insensitive)
         * @param {string} filters.sector - Exact sector match (case-insensitive)
         * @param {string} filters.sector__icontains - Sector contains substring (case-insensitive)
         * @param {string} filters.exchange__name - Exact exchange name match (case-insensitive)
         * @param {string} filters.country - Exact country match (case-insensitive)
         * @returns {Promise<object>} - Paginated response with next, previous, and results
         * @example
         * // List all stocks
         * await $store.stocksAPI.listTickers(50);
         * 
         * // List stocks with filters
         * await $store.stocksAPI.listTickers(50, null, {
         *   ticker: 'AAPL',
         *   sector: 'Technology'
         * });
         * 
         * // List stocks with contains filter
         * await $store.stocksAPI.listTickers(50, null, {
         *   ticker__icontains: 'app',
         *   country: 'US'
         * });
         */
        async listTickers(pageSize = 50, cursor = null, filters = {}) {
            try {
                this.loading = true;
                this.error = null;

                // Build query parameters
                const params = new URLSearchParams();
                if (pageSize) params.append('page_size', pageSize);
                if (cursor) params.append('cursor', cursor);

                // Add filter parameters (skip null/undefined/empty string values)
                if (filters && typeof filters === 'object') {
                    const filterKeys = [
                        'ticker',
                        'ticker__icontains',
                        'sector',
                        'sector__icontains',
                        'exchange__name',
                        'country'
                    ];

                    filterKeys.forEach(key => {
                        const value = filters[key];
                        if (value !== null && value !== undefined && value !== '') {
                            params.append(key, value);
                        }
                    });
                }

                const queryString = params.toString();
                const endpoint = queryString ? `/tickers?${queryString}` : '/tickers';

                const response = await window.api.request(endpoint, {
                    method: 'GET'
                });

                // Handle response
                if (!response.ok) {
                    const errorMessage = response.data.error?.message || response.data.detail || `Request failed with status ${response.status}`;
                    throw new Error(errorMessage);
                }

                return response.data;
            } catch (error) {
                this.error = error.message;
                console.error('Failed to list tickers:', error);
                throw error;
            } finally {
                this.loading = false;
            }
        },

        /**
         * Get detailed information about a specific stock
         * @param {string} ticker - Stock ticker symbol (e.g., "AAPL")
         * @returns {Promise<object>} - Stock details
         */
        async getTickerDetail(ticker) {
            try {
                this.loading = true;
                this.error = null;

                if (!ticker) {
                    throw new Error('Ticker symbol is required');
                }

                const response = await window.api.request(`/ticker/${ticker}/detail`, {
                    method: 'GET'
                });

                // Handle response
                if (!response.ok) {
                    const errorMessage = response.data.error?.message || response.data.detail || `Request failed with status ${response.status}`;
                    throw new Error(errorMessage);
                }

                return response.data;
            } catch (error) {
                this.error = error.message;
                console.error(`Failed to get ticker detail for ${ticker}:`, error);
                throw error;
            } finally {
                this.loading = false;
            }
        },

        /**
         * Get current ingestion status of a stock
         * Handles 404 errors gracefully (when no run exists for the stock)
         * @param {string} ticker - Stock ticker symbol (e.g., "AAPL")
         * @returns {Promise<object|null>} - Stock status with latest run information, or null if no run exists (404)
         */
        async getTickerStatus(ticker) {
            try {
                this.loading = true;
                this.error = null;

                if (!ticker) {
                    throw new Error('Ticker symbol is required');
                }

                const response = await window.api.request(`/ticker/${ticker}/status`, {
                    method: 'GET'
                });

                // 404 is expected when no run exists - return null instead of throwing
                if (response.status === 404) {
                    return null;
                }

                // Handle other errors
                if (!response.ok) {
                    const errorMessage = response.data.error?.message || response.data.detail || `Request failed with status ${response.status}`;
                    throw new Error(errorMessage);
                }

                return response.data;
            } catch (error) {
                this.error = error.message;
                console.error(`Failed to get ticker status for ${ticker}:`, error);
                throw error;
            } finally {
                this.loading = false;
            }
        },

        /**
         * Queue a stock for ingestion
         * @param {string} ticker - Stock ticker symbol (required)
         * @param {string} requestedBy - Email or identifier of requester (optional)
         * @param {string} requestId - Unique request identifier for idempotency (optional)
         * @returns {Promise<object>} - Response object with status, data, and ok flag
         */
        async queueStock(ticker, requestedBy = null, requestId = null) {
            try {
                this.loading = true;
                this.error = null;

                if (!ticker) {
                    throw new Error('Ticker symbol is required');
                }

                const requestBody = {
                    ticker: ticker,
                };

                if (requestedBy) {
                    requestBody.requested_by = requestedBy;
                }

                if (requestId) {
                    requestBody.request_id = requestId;
                }

                // Use base API request - returns {status, data, ok}
                const response = await window.api.request('/ticker/queue', {
                    method: 'POST',
                    body: JSON.stringify(requestBody)
                });

                // Return response for caller to handle different status codes
                return response;
            } catch (error) {
                this.error = error.message;
                console.error(`Failed to queue stock ${ticker}:`, error);
                throw error;
            } finally {
                this.loading = false;
            }
        }
    });
}

// Timing-safe initialization
if (window.Alpine) {
    defineStocksAPI();
} else {
    document.addEventListener('alpine:init', defineStocksAPI);
}

