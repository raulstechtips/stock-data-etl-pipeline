/**
 * Queue All Modal Store - Alpine.js Store
 * Manages state and API interactions for the queue all stocks modal component
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineQueueAllModalStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Queue all modal store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('queueAllModal', {
        // STATE
        _initialized: false,
        isOpen: false,
        selectedExchange: '',
        requestedBy: '',
        exchanges: [],
        loadingExchanges: false,
        submitting: false,
        error: null,

        /**
         * Initialize the store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Open modal and fetch exchanges
         */
        async openModal() {
            this.isOpen = true;
            this.error = null;
            
            // Reset form
            this.selectedExchange = '';
            this.requestedBy = '';
            
            // Fetch exchanges when modal opens
            await this.fetchExchanges();
        },

        /**
         * Close modal and reset state
         */
        closeModal() {
            this.isOpen = false;
            // Don't clear data immediately to allow smooth closing animation
            setTimeout(() => {
                if (!this.isOpen) {
                    this.selectedExchange = '';
                    this.requestedBy = '';
                    this.exchanges = [];
                    this.error = null;
                }
            }, 300);
        },

        /**
         * Fetch exchanges from API
         */
        async fetchExchanges() {
            try {
                this.loadingExchanges = true;
                this.error = null;

                // Use runsAPI to fetch exchanges
                const runsAPI = Alpine.store('runsAPI');
                const data = await runsAPI.listExchanges(100);
                
                // Extract exchanges from results
                this.exchanges = data.results || [];
            } catch (error) {
                this.error = error.message || 'Failed to fetch exchanges';
                console.error('Failed to fetch exchanges:', error);
                window.showToast(this.error, 'error');
            } finally {
                this.loadingExchanges = false;
            }
        },

        /**
         * Submit queue all request
         */
        async submitQueueAll() {
            try {
                this.submitting = true;

                // Build request parameters
                const requestedBy = this.requestedBy?.trim() || null;
                const exchange = this.selectedExchange?.trim() || null;

                // Use runsAPI to submit queue all request
                const runsAPI = Alpine.store('runsAPI');
                const data = await runsAPI.queueAllStocks(requestedBy, exchange);
                
                // Show success toast and close modal immediately
                const message = data.message || 'Bulk queue operation started successfully';
                window.showToast(message, 'success');
                
                // Close modal immediately on success
                this.closeModal();
            } catch (error) {
                const errorMessage = error.message || 'Failed to submit queue all request';
                console.error('Failed to submit queue all request:', error);
                window.showToast(errorMessage, 'error');
                // Keep modal open on error
            } finally {
                this.submitting = false;
            }
        }
    });
}

// CRITICAL: Timing-safe initialization
if (window.Alpine) {
    defineQueueAllModalStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineQueueAllModalStore();
    });
}

