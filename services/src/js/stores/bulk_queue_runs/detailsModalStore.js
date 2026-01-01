/**
 * Bulk Queue Run Details Modal Store - Alpine.js Store
 * Manages state and API interactions for the bulk queue run details modal component
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineBulkQueueRunDetailsModalStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Bulk queue run details modal store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('bulkQueueRunDetailsModal', {
        // STATE
        _initialized: false,
        isOpen: false,
        bulkQueueRunData: null,
        statsData: null,
        loadingStats: false,
        error: null,

        /**
         * Initialize the store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Open modal with bulk queue run data
         * @param {object} runData - Bulk queue run data object from parent component
         */
        openModal(runData) {
            if (!runData) {
                console.error('Bulk queue run data is required to open modal');
                return;
            }

            this.bulkQueueRunData = runData;
            this.isOpen = true;
            this.error = null;
            this.statsData = null;

            // Fetch stats when modal opens
            if (runData.id) {
                this.fetchStats(runData.id);
            }
        },

        /**
         * Close modal and reset state
         */
        closeModal() {
            this.isOpen = false;
            // Don't clear data immediately to allow smooth closing animation
            setTimeout(() => {
                if (!this.isOpen) {
                    this.bulkQueueRunData = null;
                    this.statsData = null;
                    this.error = null;
                }
            }, 300);
        },

        /**
         * Fetch bulk queue run stats using runsAPI
         * @param {string} bulkQueueRunId - Bulk queue run UUID
         */
        async fetchStats(bulkQueueRunId) {
            if (!bulkQueueRunId) {
                this.error = 'Bulk queue run ID is required';
                return;
            }

            try {
                this.loadingStats = true;
                this.error = null;

                // Use runs API
                const runsAPI = Alpine.store('runsAPI');
                const data = await runsAPI.getBulkQueueRunStats(bulkQueueRunId);
                
                this.statsData = data;
            } catch (error) {
                this.error = error.message || 'Failed to fetch bulk queue run stats';
                console.error(`Failed to get bulk queue run stats for ${bulkQueueRunId}:`, error);
                window.showToast(this.error, 'error');
            } finally {
                this.loadingStats = false;
            }
        },

        /**
         * Get badge color class for ingestion state
         * @param {string} state - Ingestion state
         * @returns {string} - Tailwind CSS classes for badge
         */
        getStateBadgeClass(state) {
            if (!state) return 'bg-theme-tertiary text-theme-primary';
            
            const stateUpper = state.toUpperCase();
            
            // Terminal states
            if (stateUpper === 'DONE') {
                return 'bg-success text-success-text';
            }
            if (stateUpper === 'FAILED') {
                return 'bg-error text-error-text';
            }
            
            // Active processing states (currently working)
            if (stateUpper === 'FETCHING' || stateUpper === 'DELTA_RUNNING') {
                return 'bg-info text-info-text';
            }
            
            // Intermediate completion states
            if (stateUpper === 'FETCHED' || stateUpper === 'DELTA_FINISHED') {
                return 'bg-info text-info-text';
            }
            
            // Queued states (waiting to be processed)
            if (stateUpper.includes('QUEUED')) {
                return 'bg-warning text-warning-text';
            }
            
            // Default fallback
            return 'bg-theme-tertiary text-theme-primary';
        },

        /**
         * Get progress bar color class for ingestion state
         * @param {string} state - Ingestion state
         * @returns {string} - Tailwind CSS classes for progress bar
         */
        getProgressBarClass(state) {
            if (!state) return 'bg-theme-tertiary';
            
            const stateUpper = state.toUpperCase();
            
            // Terminal states
            if (stateUpper === 'DONE') {
                return 'bg-success';
            }
            if (stateUpper === 'FAILED') {
                return 'bg-error';
            }
            
            // Active processing states (currently working)
            if (stateUpper === 'FETCHING' || stateUpper === 'DELTA_RUNNING') {
                return 'bg-info';
            }
            
            // Intermediate completion states
            if (stateUpper === 'FETCHED' || stateUpper === 'DELTA_FINISHED') {
                return 'bg-info';
            }
            
            // Queued states (waiting to be processed)
            if (stateUpper.includes('QUEUED')) {
                return 'bg-warning';
            }
            
            // Default fallback
            return 'bg-theme-tertiary';
        },

        /**
         * Format state name for display
         * @param {string} state - Ingestion state
         * @returns {string} - Formatted state name
         */
        formatStateName(state) {
            if (!state) return 'N/A';
            return state.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
        },

        /**
         * Calculate progress percentage for a state
         * @param {number} stateCount - Count for the state
         * @param {number} totalStocks - Total stocks (anchor for progress)
         * @returns {number} - Progress percentage (0-100)
         */
        calculateProgress(stateCount, totalStocks) {
            if (!totalStocks || totalStocks === 0) return 0;
            return Math.round((stateCount / totalStocks) * 100);
        }
    });
}

// CRITICAL: Timing-safe initialization
if (window.Alpine) {
    defineBulkQueueRunDetailsModalStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineBulkQueueRunDetailsModalStore();
    });
}

