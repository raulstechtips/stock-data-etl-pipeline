/**
 * Runs API Store
 * Alpine.js store for ingestion run API endpoints
 * Handles run listing, filtering by ticker, and run detail retrieval
 */

/**
 * Define the runs API store
 */
function defineRunsAPI() {
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Runs API store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('runsAPI', {
        // State
        loading: false,
        error: null,
        _initialized: false,

        /**
         * Initialize the runs API store (idempotent)
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
        },

        /**
         * Build query parameters for API requests
         * @private
         * @param {number} pageSize - Number of items per page
         * @param {string} cursor - Pagination cursor
         * @param {object} filters - Filter parameters
         * @param {string[]} stringFilterKeys - Array of string filter keys to process
         * @param {string[]} booleanFilterKeys - Array of boolean filter keys to process
         * @returns {URLSearchParams} - Built query parameters
         */
        _buildQueryParams(pageSize, cursor, filters, stringFilterKeys, booleanFilterKeys) {
            const params = new URLSearchParams();
            if (pageSize) params.append('page_size', pageSize);
            if (cursor) params.append('cursor', cursor);

            // Add filter parameters (skip null/undefined/empty string values)
            if (filters && typeof filters === 'object') {
                // Handle string filters
                stringFilterKeys.forEach(key => {
                    const value = filters[key];
                    if (value !== null && value !== undefined && value !== '') {
                        params.append(key, value);
                    }
                });

                // Handle boolean filters (convert to string)
                booleanFilterKeys.forEach(key => {
                    const value = filters[key];
                    if (value !== null && value !== undefined) {
                        params.append(key, String(value));
                    }
                });
            }

            return params;
        },

        /**
         * List all ingestion runs across all stocks with pagination and optional filters
         * @param {number} pageSize - Number of items per page (max: 100)
         * @param {string} cursor - Pagination cursor from previous response
         * @param {object} filters - Optional filter parameters
         * @param {string} filters.ticker - Exact ticker match (case-insensitive)
         * @param {string} filters.ticker__icontains - Ticker contains substring (case-insensitive)
         * @param {string} filters.state - Ingestion state (e.g., "PENDING", "RUNNING", "DONE", "FAILED")
         * @param {string} filters.requested_by - Exact requester identifier match (case-insensitive)
         * @param {string} filters.requested_by__icontains - Requester contains substring (case-insensitive)
         * @param {string} filters.created_after - Filter runs created after this date (ISO 8601 format, e.g., "2025-01-01T00:00:00Z")
         * @param {string} filters.created_before - Filter runs created before this date (ISO 8601 format, e.g., "2025-12-31T23:59:59Z")
         * @param {boolean} filters.is_terminal - Filter terminal states (true for DONE/FAILED, false for non-terminal)
         * @param {boolean} filters.is_in_progress - Filter in-progress runs (true for non-terminal, false for terminal)
         * @param {string} filters.run_id - Filter by ingestion run UUID
         * @param {string} filters.bulk_queue_run - Filter by BulkQueueRun UUID
         * @returns {Promise<object>} - Paginated response with next, previous, and results
         * @example
         * // List all runs
         * await $store.runsAPI.listRuns(50);
         * 
         * // List runs with filters
         * await $store.runsAPI.listRuns(50, null, {
         *   ticker: 'AAPL',
         *   state: 'FAILED',
         *   is_terminal: true
         * });
         * 
         * // List runs with date range
         * await $store.runsAPI.listRuns(50, null, {
         *   created_after: '2025-01-01T00:00:00Z',
         *   created_before: '2025-12-31T23:59:59Z'
         * });
         */
        async listRuns(pageSize = 50, cursor = null, filters = {}) {
            try {
                this.loading = true;
                this.error = null;

                // Build query parameters
                const stringFilterKeys = [
                    'ticker',
                    'ticker__icontains',
                    'state',
                    'requested_by',
                    'requested_by__icontains',
                    'created_after',
                    'created_before',
                    'run_id',
                    'bulk_queue_run'
                ];
                const booleanFilterKeys = ['is_terminal', 'is_in_progress'];
                const params = this._buildQueryParams(pageSize, cursor, filters, stringFilterKeys, booleanFilterKeys);

                const queryString = params.toString();
                const endpoint = queryString ? `/runs?${queryString}` : '/runs';

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
                console.error('Failed to list runs:', error);
                throw error;
            } finally {
                this.loading = false;
            }
        },

        /**
         * List ingestion runs for a specific stock ticker with pagination and optional filters
         * @param {string} ticker - Stock ticker symbol (e.g., "AAPL")
         * @param {number} pageSize - Number of items per page (max: 100)
         * @param {string} cursor - Pagination cursor from previous response
         * @param {object} filters - Optional filter parameters
         * @param {string} filters.ticker - Exact ticker match (case-insensitive) - Note: redundant when ticker is in URL
         * @param {string} filters.ticker__icontains - Ticker contains substring (case-insensitive) - Note: redundant when ticker is in URL
         * @param {string} filters.state - Ingestion state (e.g., "PENDING", "RUNNING", "DONE", "FAILED")
         * @param {string} filters.requested_by - Exact requester identifier match (case-insensitive)
         * @param {string} filters.requested_by__icontains - Requester contains substring (case-insensitive)
         * @param {string} filters.created_after - Filter runs created after this date (ISO 8601 format, e.g., "2025-01-01T00:00:00Z")
         * @param {string} filters.created_before - Filter runs created before this date (ISO 8601 format, e.g., "2025-12-31T23:59:59Z")
         * @param {boolean} filters.is_terminal - Filter terminal states (true for DONE/FAILED, false for non-terminal)
         * @param {boolean} filters.is_in_progress - Filter in-progress runs (true for non-terminal, false for terminal)
         * @param {string} filters.run_id - Filter by ingestion run UUID
         * @param {string} filters.bulk_queue_run - Filter by BulkQueueRun UUID
         * @returns {Promise<object>} - Paginated response with next, previous, and results
         * @example
         * // List runs for a ticker
         * await $store.runsAPI.listRunsByTicker('AAPL', 50);
         * 
         * // List runs for a ticker with filters
         * await $store.runsAPI.listRunsByTicker('AAPL', 50, null, {
         *   state: 'FAILED',
         *   is_terminal: true
         * });
         * 
         * // List runs for a ticker with date range
         * await $store.runsAPI.listRunsByTicker('AAPL', 50, null, {
         *   created_after: '2025-01-01T00:00:00Z',
         *   created_before: '2025-12-31T23:59:59Z'
         * });
         */
        async listRunsByTicker(ticker, pageSize = 50, cursor = null, filters = {}) {
            try {
                this.loading = true;
                this.error = null;

                if (!ticker) {
                    throw new Error('Ticker symbol is required');
                }

                // Build query parameters
                const stringFilterKeys = [
                    'ticker',
                    'ticker__icontains',
                    'state',
                    'requested_by',
                    'requested_by__icontains',
                    'created_after',
                    'created_before',
                    'run_id',
                    'bulk_queue_run'
                ];
                const booleanFilterKeys = ['is_terminal', 'is_in_progress'];
                const params = this._buildQueryParams(pageSize, cursor, filters, stringFilterKeys, booleanFilterKeys);

                const queryString = params.toString();
                const endpoint = queryString 
                    ? `/runs/ticker/${ticker}?${queryString}` 
                    : `/runs/ticker/${ticker}`;

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
                console.error(`Failed to list runs for ticker ${ticker}:`, error);
                throw error;
            } finally {
                this.loading = false;
            }
        },

        /**
         * Get detailed information about a specific ingestion run
         * @param {string} runId - Ingestion run UUID
         * @returns {Promise<object>} - Run details
         */
        async getRunDetail(runId) {
            try {
                this.loading = true;
                this.error = null;

                if (!runId) {
                    throw new Error('Run ID is required');
                }

                const response = await window.api.request(`/run/${runId}/detail`, {
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
                console.error(`Failed to get run detail for ${runId}:`, error);
                throw error;
            } finally {
                this.loading = false;
            }
        },

        /**
         * Get detailed statistics for a specific bulk queue run
         * @param {string} bulkQueueRunId - Bulk queue run UUID
         * @returns {Promise<object>} - Bulk queue run details with ingestion_run_stats
         */
        async getBulkQueueRunStats(bulkQueueRunId) {
            try {
                this.loading = true;
                this.error = null;

                if (!bulkQueueRunId) {
                    throw new Error('Bulk queue run ID is required');
                }

                const response = await window.api.request(`/bulk-queue-runs/${bulkQueueRunId}/stats`, {
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
                console.error(`Failed to get bulk queue run stats for ${bulkQueueRunId}:`, error);
                throw error;
            } finally {
                this.loading = false;
            }
        },

        /**
         * List all bulk queue runs with pagination and optional filters
         * @param {number} pageSize - Number of items per page (max: 100)
         * @param {string} cursor - Pagination cursor from previous response
         * @param {object} filters - Optional filter parameters
         * @param {string} filters.requested_by - Exact requester match (case-insensitive)
         * @param {string} filters.requested_by__icontains - Requester contains substring (case-insensitive)
         * @param {string} filters.created_after - Filter runs created after this date (ISO 8601 format, e.g., "2025-01-01T00:00:00Z")
         * @param {string} filters.created_before - Filter runs created before this date (ISO 8601 format, e.g., "2025-12-31T23:59:59Z")
         * @param {string} filters.started_at_after - Filter runs started after this date (ISO 8601 format)
         * @param {string} filters.started_at_before - Filter runs started before this date (ISO 8601 format)
         * @param {string} filters.completed_at_after - Filter runs completed after this date (ISO 8601 format)
         * @param {string} filters.completed_at_before - Filter runs completed before this date (ISO 8601 format)
         * @param {boolean} filters.is_completed - Filter by completion status (true/false)
         * @param {boolean} filters.has_errors - Filter by error presence (true/false)
         * @returns {Promise<object>} - Paginated response with next, previous, and results
         * @example
         * // List all bulk queue runs
         * await $store.runsAPI.listBulkQueueRuns(50);
         * 
         * // List bulk queue runs with filters
         * await $store.runsAPI.listBulkQueueRuns(50, null, {
         *   requested_by: 'admin@example.com',
         *   is_completed: true,
         *   has_errors: false
         * });
         * 
         * // List bulk queue runs with date range
         * await $store.runsAPI.listBulkQueueRuns(50, null, {
         *   created_after: '2025-01-01T00:00:00Z',
         *   created_before: '2025-12-31T23:59:59Z'
         * });
         */
        async listBulkQueueRuns(pageSize = 50, cursor = null, filters = {}) {
            try {
                this.loading = true;
                this.error = null;

                // Build query parameters
                const stringFilterKeys = [
                    'requested_by',
                    'requested_by__icontains',
                    'created_after',
                    'created_before',
                    'started_at_after',
                    'started_at_before',
                    'completed_at_after',
                    'completed_at_before'
                ];
                const booleanFilterKeys = ['is_completed', 'has_errors'];
                const params = this._buildQueryParams(pageSize, cursor, filters, stringFilterKeys, booleanFilterKeys);

                const queryString = params.toString();
                const endpoint = queryString ? `/bulk-queue-runs?${queryString}` : '/bulk-queue-runs';

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
                console.error('Failed to list bulk queue runs:', error);
                throw error;
            } finally {
                this.loading = false;
            }
        }
    });
}

// Timing-safe initialization
if (window.Alpine) {
    defineRunsAPI();
} else {
    document.addEventListener('alpine:init', defineRunsAPI);
}

