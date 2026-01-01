/**
 * Run Details Modal Store - Alpine.js Store
 * Manages state for the run details modal component
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineRunDetailsModalStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Run details modal store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('runDetailsModal', {
        // STATE
        _initialized: false,
        isOpen: false,
        runData: null,

        /**
         * Initialize the store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Open modal with run data
         * @param {object} runData - Complete run data object from parent component
         */
        openModal(runData) {
            if (!runData) {
                console.error('Run data is required to open modal');
                return;
            }

            this.runData = runData;
            this.isOpen = true;
        },

        /**
         * Close modal and reset state
         */
        closeModal() {
            this.isOpen = false;
            // Don't clear runData immediately to allow smooth closing animation
            // Animation duration is 200ms, wait a bit longer to ensure animation completes
            setTimeout(() => {
                if (!this.isOpen) {
                    this.runData = null;
                }
            }, 250);
        },

        /**
         * Get badge color class for run state
         * Matches the pattern used in ingestion_run_card.html
         * @param {string} state - Run state
         * @returns {string} - Tailwind CSS classes for badge
         */
        getStateBadgeClass(state) {
            if (!state) return 'bg-theme-tertiary text-theme-primary';
            
            const stateUpper = state.toUpperCase();
            
            // Terminal success state
            if (stateUpper === 'DONE') {
                return 'bg-success text-success-text';
            }
            
            // Terminal error state
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
            return 'bg-info text-info-text';
        }
    });
}

// CRITICAL: Timing-safe initialization
if (window.Alpine) {
    defineRunDetailsModalStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineRunDetailsModalStore();
    });
}

