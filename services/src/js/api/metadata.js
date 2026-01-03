/**
 * Metadata API Store
 * Alpine.js store for metadata API endpoints
 * Handles metadata listing, and metadata detail retrieval
 */

/**
 * Define the metadata API store
 */
function defineMetadataAPI() {
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Metadata API store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('metadataAPI', {
        // State
        loading: false,
        error: null,
        _initialized: false,

        /**
         * Initialize the metadata API store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * List all exchanges with pagination and optional filters
         * @param {number} pageSize - Number of items per page (max: 100)
         * @param {string} cursor - Pagination cursor from previous response
         * @param {object} filters - Optional filter parameters
         * @param {string} filters.name - Exact exchange name match (case-insensitive)
         * @param {string} filters.name__icontains - Exchange name contains substring (case-insensitive)
         * @returns {Promise<object>} - Paginated response with next, previous, and results
         * @example
         * // List all exchanges
         * await $store.metadataAPI.listExchanges();
         * 
         * // List exchanges with filters
         * await $store.metadataAPI.listExchanges(100, null, {
         *   name: 'NASDAQ'
         * });
         */
        async listExchanges(pageSize = 50, cursor = null, filters = {}) {
            try {
                this.loading = true;
                this.error = null;

                // Build query parameters
                const stringFilterKeys = ['name', 'name__icontains'];
                const booleanFilterKeys = [];
                const params = window.api.buildQueryParams(pageSize, cursor, filters, stringFilterKeys, booleanFilterKeys);

                const queryString = params.toString();
                const endpoint = queryString ? `/exchanges?${queryString}` : '/exchanges';

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
                console.error('Failed to list exchanges:', error);
                throw error;
            } finally {
                this.loading = false;
            }
        },

        /**
         * List all sectors with pagination and optional filters
         * @param {number} pageSize - Number of items per page (max: 100)
         * @param {string} cursor - Pagination cursor from previous response
         * @param {object} filters - Optional filter parameters
         * @param {string} filters.name - Exact sector name match (case-insensitive)
         * @param {string} filters.name__icontains - Sector name contains substring (case-insensitive)
         * @returns {Promise<object>} - Paginated response with next, previous, and results
         * @example
         * // List all sectors
         * await $store.metadataAPI.listSectors();
         * 
         * // List sectors with filters
         * await $store.metadataAPI.listSectors(100, null, {
         *   name: 'Information Technology'
         * });
         */
        async listSectors(pageSize = 50, cursor = null, filters = {}) {
            try {
                this.loading = true;
                this.error = null;

                // Build query parameters
                const stringFilterKeys = ['name', 'name__icontains'];
                const booleanFilterKeys = [];
                const params = window.api.buildQueryParams(pageSize, cursor, filters, stringFilterKeys, booleanFilterKeys);

                const queryString = params.toString();
                const endpoint = queryString ? `/sectors?${queryString}` : '/sectors';

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
                console.error('Failed to list sectors:', error);
                throw error;
            } finally {
                this.loading = false;
            }
        },

    });
}

// Timing-safe initialization
if (window.Alpine) {
    defineMetadataAPI();
} else {
    document.addEventListener('alpine:init', defineMetadataAPI);
}

