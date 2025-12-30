/**
 * Auth Store - Alpine.js Store
 * Manages authentication state and logout functionality.
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineAuthStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Auth store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('auth', {
        // STATE
        _initialized: false,
        loading: false,
        error: null,

        /**
         * Initialize the auth store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Logout user
         * Calls the auth API logout endpoint, handles success/error states,
         * and redirects to '/' on successful logout
         * @returns {Promise<void>}
         * @example
         * // Logout user
         * await $store.auth.logout();
         */
        async logout() {
            try {
                this.loading = true;
                this.error = null;

                // Call auth API logout endpoint
                const response = await Alpine.store('authAPI').logout();

                // Handle response
                if (!response.ok) {
                    const errorMessage = response.data?.error?.message || response.data?.detail || `Logout failed with status ${response.status}`;
                    throw new Error(errorMessage);
                }

                // Show success toast
                 if (window.showToast) {
                    window.showToast(
                        'Logged out successfully',
                        'success'
                    );
                }

                // Success - redirect to root
                window.location.href = '/';
            } catch (error) {
                this.error = error.message;
                console.error('Logout failed:', error);
                
                // Show error toast notification
                if (window.showToast) {
                    window.showToast(
                        error.message || 'Failed to logout. Please try again.',
                        'error'
                    );
                }
            } finally {
                this.loading = false;
            }
        }
    });
}

// CRITICAL: Timing-safe initialization
if (window.Alpine) {
    defineAuthStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineAuthStore();
    });
}

