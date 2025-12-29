/**
 * API Base Client
 * Core request functionality for all API modules
 * Includes CSRF token handling and consistent error handling
 */

const api = {
    baseURL: '/api',
    
    /**
     * Get CSRF token from cookie (SINGLE SOURCE OF TRUTH)
     * Always reads from cookie as Django keeps this updated.
     * Do not read from DOM or meta tags as they can become stale.
     * @returns {string} - CSRF token
     */
    getCsrfToken() {
        const name = 'csrftoken';
        let cookieValue = null;
        
        if (document.cookie && document.cookie !== '') {
            const cookies = document.cookie.split(';');
            for (let i = 0; i < cookies.length; i++) {
                const cookie = cookies[i].trim();
                // Check if this cookie string begins with the name we want
                if (cookie.substring(0, name.length + 1) === (name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                }
            }
        }
        
        return cookieValue || '';
    },

    /**
     * Make API requests with full response control
     * Returns status, data, and ok flag - caller decides how to handle errors
     * @param {string} endpoint - API endpoint path
     * @param {object} options - Fetch options
     * @returns {Promise<{status: number, data: object|null, ok: boolean}>} - Response object
     */
    async request(endpoint, options = {}) {
        try {
            // Get CSRF token from cookies
            const csrfToken = this.getCsrfToken();
            
            const url = `${this.baseURL}${endpoint}`;
            
            const response = await fetch(url, {
                ...options,
                headers: {
                    ...options.headers,
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrfToken,
                },
                credentials: 'include',
            });
            
            // Handle empty responses (204 No Content)
            if (response.status === 204) {
                return {
                    status: 204,
                    data: { success: true },
                    ok: response.ok
                };
            }
            
            // Parse JSON response
            const data = await response.json();
            
            // Return raw response - let caller decide how to handle
            return {
                status: response.status,
                data: data,
                ok: response.ok
            };
        } catch (error) {
            console.error('API Error:', error);
            throw error;
        }
    }
};

// Make api globally available
window.api = api;
