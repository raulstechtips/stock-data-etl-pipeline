/**
 * Stock Details Modal Store - Alpine.js Store
 * Manages state and API interactions for the stock details modal component
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineStockDetailsModalStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Stock details modal store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('stockDetailsModal', {
        // STATE
        _initialized: false,
        isOpen: false,
        stockData: null,
        runStatus: null,
        loadingStatus: false,
        loadingQueue: false,
        error: null,

        /**
         * Initialize the store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Open modal with stock data
         * @param {object} stockData - Stock data object from parent component
         */
        openModal(stockData) {
            if (!stockData) {
                console.error('Stock data is required to open modal');
                return;
            }

            this.stockData = stockData;
            this.isOpen = true;
            this.error = null;
            this.runStatus = null;

            // Fetch stock status when modal opens
            if (stockData.ticker) {
                this.fetchStockStatus(stockData.ticker);
            }
        },

        /**
         * Close modal and reset state
         */
        closeModal() {
            this.isOpen = false;
            // Don't clear stockData immediately to allow smooth closing animation
            setTimeout(() => {
                if (!this.isOpen) {
                    this.stockData = null;
                    this.runStatus = null;
                    this.error = null;
                }
            }, 300);
        },

        /**
         * Fetch stock status using stocksAPI
         * Handles 404 errors gracefully (when stock status is not found)
         * @param {string} ticker - Stock ticker symbol
         */
        async fetchStockStatus(ticker) {
            if (!ticker) {
                this.error = 'Ticker symbol is required';
                return;
            }

            try {
                this.loadingStatus = true;
                this.error = null;

                // Use stocks API - it handles 404 gracefully and returns null
                const stocksAPI = Alpine.store('stocksAPI');
                const data = await stocksAPI.getTickerStatus(ticker);
                
                // getTickerStatus returns null for 404 (no run exists)
                this.runStatus = data;
            } catch (error) {
                this.error = error.message || 'Failed to fetch stock status';
                console.error(`Failed to get ticker status for ${ticker}:`, error);
                window.showToast(this.error, 'error');
            } finally {
                this.loadingStatus = false;
            }
        },

        /**
         * Queue stock for ingestion using stocksAPI
         * Handles different response statuses: 201, 200, 400, 409, 500
         * @param {string} ticker - Stock ticker symbol (required)
         * @param {string} requestedBy - Email or identifier of requester (optional)
         * @param {string} requestId - Unique request identifier for idempotency (optional)
         */
        async queueStock(ticker, requestedBy = null, requestId = null) {
            if (!ticker) {
                const error = 'Ticker symbol is required';
                this.error = error;
                window.showToast(error, 'error');
                return;
            }

            try {
                this.loadingQueue = true;
                this.error = null;

                // Use stocks API
                const stocksAPI = Alpine.store('stocksAPI');
                const response = await stocksAPI.queueStock(ticker, requestedBy, requestId);

                // Handle success cases (200 OK or 201 Created)
                if (response.status === 200 || response.status === 201) {
                    // Refresh status to show new/existing run details
                    await this.fetchStockStatus(ticker);
                    window.showToast(`Stock ${ticker} queued successfully`, 'success');
                    return;
                }

                // Handle error cases
                const data = response.data;
                
                if (response.status === 400) {
                    // Validation error
                    const errorMessage = data.error?.message || 'Invalid request. Please check the ticker symbol.';
                    this.error = errorMessage;
                    window.showToast(errorMessage, 'error');
                } else if (response.status === 409) {
                    // Race condition - run was created by another request
                    const errorMessage = data.error?.message || 'A run for this stock was just created. Refreshing status...';
                    this.error = errorMessage;
                    window.showToast(errorMessage, 'warning');
                    // Refresh status to get the existing run
                    await this.fetchStockStatus(ticker);
                } else if (response.status === 500) {
                    // Broker error
                    const errorMessage = data.message || data.error?.message || 'Failed to queue task. Please try again later.';
                    this.error = errorMessage;
                    window.showToast(errorMessage, 'error');
                } else {
                    // Other errors
                    const errorMessage = data.error?.message || data.message || data.detail || `Request failed with status ${response.status}`;
                    this.error = errorMessage;
                    window.showToast(errorMessage, 'error');
                }
            } catch (error) {
                // Network or parsing errors
                const errorMessage = error.message || 'Failed to queue stock. Please try again.';
                this.error = errorMessage;
                window.showToast(errorMessage, 'error');
                console.error(`Failed to queue stock ${ticker}:`, error);
            } finally {
                this.loadingQueue = false;
            }
        },

        /**
         * Get badge color class for run state
         * @param {string} state - Run state
         * @returns {string} - Tailwind CSS classes for badge
         */
        getStateBadgeClass(state) {
            if (!state) return 'bg-theme-tertiary text-theme-primary';
            
            const stateUpper = state.toUpperCase();
            
            // Terminal states
            if (stateUpper === 'COMPLETED') {
                return 'bg-success text-success-text';
            }
            if (stateUpper === 'FAILED') {
                return 'bg-error text-error-text';
            }
            
            // Active processing states (currently working)
            if (stateUpper === 'FETCHING' || stateUpper === 'TRANSFORMING' || stateUpper === 'LOADING') {
                return 'bg-info text-info-text';
            }
            
            // Phase completion states (intermediate, still in progress)
            if (stateUpper === 'FETCH_COMPLETED' || stateUpper === 'TRANSFORM_COMPLETED') {
                return 'bg-info text-info-text';
            }
            
            // Queued states (waiting to be processed)
            if (stateUpper.includes('QUEUED')) {
                return 'bg-warning text-warning-text';
            }
            
            // Pending state
            if (stateUpper === 'PENDING') {
                return 'bg-theme-tertiary text-theme-primary';
            }
            
            // Default fallback
            return 'bg-theme-tertiary text-theme-primary';
        }
    });
}

// CRITICAL: Timing-safe initialization
if (window.Alpine) {
    defineStockDetailsModalStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineStockDetailsModalStore();
    });
}

