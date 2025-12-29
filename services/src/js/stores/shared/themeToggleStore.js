/**
 * Theme Toggle Store - Alpine.js Store
 * Manages theme state (light/dark mode) and theme switching functionality.
 * Follows Alpine.store() pattern for Django template compatibility
 */

function defineThemeToggleStore() {
    // Use window.Alpine since Alpine.js is loaded via CDN
    if (!window.Alpine) {
        console.error('Alpine.js is not available. Theme toggle store cannot be initialized.');
        return;
    }
    
    const Alpine = window.Alpine;
    
    Alpine.store('themeToggle', {
        // STATE
        _initialized: false,
        currentTheme: 'light', // Default to light mode

        /**
         * Initialize theme from localStorage or default to light
         * This should be called before hiding the loading screen
         */
        init() {
            if (this._initialized) return;
            this._initialized = true;
            
            // Get saved theme from localStorage or default to light
            const savedTheme = localStorage.getItem('theme') || 'light';
            this.currentTheme = savedTheme;
            
            // Apply theme to DOM
            this._applyTheme(savedTheme);
        },

        /**
         * Apply theme to the DOM
         * @param {string} theme - 'light' or 'dark'
         * @private
         */
        _applyTheme(theme) {
            const html = document.documentElement;
            
            // Set data-theme attribute for CSS custom properties
            html.setAttribute('data-theme', theme);
            
            // Add/remove dark class for Tailwind dark mode
            if (theme === 'dark') {
                html.classList.add('dark');
            } else {
                html.classList.remove('dark');
            }
        },

        /**
         * Get current theme
         * @returns {string} 'light' or 'dark'
         */
        getTheme() {
            return this.currentTheme;
        },

        /**
         * Set theme
         * @param {string} theme - 'light' or 'dark'
         */
        setTheme(theme) {
            if (theme !== 'light' && theme !== 'dark') {
                console.warn(`Invalid theme: ${theme}. Using 'light' instead.`);
                theme = 'light';
            }
            
            this.currentTheme = theme;
            localStorage.setItem('theme', theme);
            this._applyTheme(theme);
        },

        /**
         * Toggle between light and dark themes
         * @returns {string} The new theme
         */
        toggle() {
            const newTheme = this.currentTheme === 'dark' ? 'light' : 'dark';
            this.setTheme(newTheme);
            return newTheme;
        },

        /**
         * Check if current theme is dark
         * @returns {boolean}
         */
        isDark() {
            return this.currentTheme === 'dark';
        },

        /**
         * Check if current theme is light
         * @returns {boolean}
         */
        isLight() {
            return this.currentTheme === 'light';
        }
    });
}

// CRITICAL: Timing-safe initialization
if (window.Alpine) {
    defineThemeToggleStore();
} else {
    document.addEventListener('alpine:init', () => {
        defineThemeToggleStore();
    });
}

