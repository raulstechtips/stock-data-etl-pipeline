/**
 * Global Utility Functions
 * Helper functions available throughout the application
 */

/**
 * Show toast notification
 * @param {string} message - Message to display
 * @param {string} type - Type: 'success', 'error', 'warning', 'info'
 * @param {number} duration - Duration in milliseconds (default: 5000)
 */
window.showToast = function(message, type = 'info', duration = 5000) {
    window.dispatchEvent(new CustomEvent('show-toast', {
        detail: { message, type, duration }
    }));
};

/**
 * Show confirmation modal (Promise-based)
 * @param {string} title - Modal title
 * @param {string} message - Modal message
 * @param {string} confirmText - Confirm button text (default: 'Confirm')
 * @param {string} cancelText - Cancel button text (default: 'Cancel')
 * @param {string} type - Type: 'danger', 'warning', 'info' (default: 'danger')
 * @returns {Promise<boolean>} - Resolves to true if confirmed, false if cancelled
 */
window.showConfirm = function(title, message, confirmText = 'Confirm', cancelText = 'Cancel', type = 'danger') {
    return new Promise((resolve) => {
        window.dispatchEvent(new CustomEvent('confirm-modal', {
            detail: {
                title,
                message,
                type,
                confirmText,
                cancelText,
                onConfirm: () => resolve(true),
                onCancel: () => resolve(false)
            }
        }));
    });
};
