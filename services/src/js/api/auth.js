/**
 * Auth API Store
 * Alpine.js store for authentication API endpoints
 * Handles logout functionality
 */

/**
 * Define the auth API store
 */
function defineAuthAPI() {
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Auth API store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('authAPI', {
        // State
        loading: false,
        error: null,
        _initialized: false,

        /**
         * Initialize the auth API store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Logout user by sending POST request to Django allauth logout endpoint
         * @returns {Promise<{status: number, data: object|null, ok: boolean}>} - Response object
         * @example
         * // Logout user
         * await $store.authAPI.logout();
         */
        async logout() {
            try {
                this.loading = true;
                this.error = null;
                
                // Use base API request - returns {status, data, ok}
                const response = await window.api.request('/accounts/logout/', {
                    method: 'POST',
                });

                // Return response in same format as base.js
                // response.data is already parsed (or null for non-JSON responses)
                return {
                    status: response.status,
                    data: response.data,
                    ok: response.ok
                };
            } catch (error) {
                this.error = error.message;
                console.error('Failed to logout:', error);
                throw error;
            } finally {
                this.loading = false;
            }
        }
    });
}

// Timing-safe initialization
if (window.Alpine) {
    defineAuthAPI();
} else {
    document.addEventListener('alpine:init', defineAuthAPI);
}

