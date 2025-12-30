/**
 * Base Store - Alpine.js Store
 * Manages base state: page loading.
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineBaseStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Base store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('base', {
        // STATE
        _initialized: false,
        _loadingHidden: false,

        init() {
            if (this._initialized) return;
            this._initialized = true;
            
            // Initialize theme toggle before hiding loading screen
            // This ensures theme is applied before content is visible
            if (Alpine.store('themeToggle')) {
                Alpine.store('themeToggle').init();
            }

            // Ensure authAPI store is initialized
            if (Alpine.store('authAPI')) {
                Alpine.store('authAPI').init();
            }
            
            // Hide page loading overlay
            this.hidePageLoading();
        },

        hidePageLoading() {
            if (this._loadingHidden) return;
            this._loadingHidden = true;
            
            const overlay = document.getElementById('page-loading-overlay');
            const mainContent = document.getElementById('main-content');
            
            if (overlay && mainContent) {
                // Fade out overlay
                overlay.style.transition = 'opacity 0.3s ease-out';
                overlay.style.opacity = '0';
                
                // Fade in main content
                mainContent.style.transition = 'opacity 0.3s ease-in';
                mainContent.style.opacity = '1';
                
                // Hide overlay after animation
                setTimeout(() => {
                    overlay.style.display = 'none';
                }, 300);
            }
        },
    });
}

// CRITICAL: Timing-safe initialization
if (window.Alpine) {
    defineBaseStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineBaseStore();
    });
}
