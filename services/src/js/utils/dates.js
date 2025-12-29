/**
 * Date Utility Functions
 * Helper functions for date formatting, parsing, and manipulation
 */

const dateUtils = {
    /**
     * Check if a string is an ISO 8601 timestamp with time component
     * @param {string} str - String to check
     * @returns {boolean} - True if ISO timestamp format
     * @private
     */
    _isISOTimestamp(str) {
        // Matches ISO 8601 with time: 2025-10-14T15:30:00 or 2025-10-14T15:30:00Z or with timezone
        return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(str);
    },
    
    /**
     * Check if a string is a date-only format (YYYY-MM-DD)
     * @param {string} str - String to check
     * @returns {boolean} - True if date-only format
     * @private
     */
    _isDateOnly(str) {
        return /^\d{4}-\d{2}-\d{2}$/.test(str);
    },
    
    /**
     * Parse string to Date with appropriate timezone handling
     * @param {string} str - Date string
     * @returns {Date} - Parsed date
     * @private
     */
    _parseString(str) {
        if (this._isDateOnly(str)) {
            // Date-only: parse as local midnight
            return this.parseDate(str);
        } else if (this._isISOTimestamp(str)) {
            // ISO timestamp: parse as UTC (native behavior)
            return new Date(str);
        } else {
            // Fallback to native parser
            return new Date(str);
        }
    },
    
    /**
     * Format a date to YYYY-MM-DD
     * @param {Date|string} date - Date object or string
     * @returns {string} - Formatted date (YYYY-MM-DD)
     */
    formatDate(date) {
        // If it's already a YYYY-MM-DD string, return it
        if (typeof date === 'string' && this._isDateOnly(date)) {
            return date;
        }
        
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        const year = d.getFullYear();
        const month = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    },
    
    /**
     * Format a date to human-readable format
     * @param {Date|string} date - Date object or string
     * @param {object} options - Intl.DateTimeFormat options
     * @returns {string} - Formatted date (e.g., "Monday, October 13, 2025")
     */
    formatDateLong(date, options = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        return d.toLocaleDateString('en-US', options);
    },
    
    /**
     * Format a date to short format
     * @param {Date|string} date - Date object or string
     * @returns {string} - Formatted date (e.g., "Oct 13, 2025")
     */
    formatDateShort(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        return d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    },
    
    /**
     * Format a date to medium format
     * @param {Date|string} date - Date object or string
     * @returns {string} - Formatted date (e.g., "October 13, 2025")
     */
    formatDateMedium(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        return d.toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
    },
    
    /**
     * Get today's date as Date object (time set to 00:00:00)
     * @returns {Date} - Today's date
     */
    getToday() {
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        return today;
    },
    
    /**
     * Get today's date as YYYY-MM-DD string
     * @returns {string} - Today's date
     */
    getTodayString() {
        return this.formatDate(this.getToday());
    },
    
    /**
     * Parse a date string to Date object
     * @param {string} dateString - Date string (YYYY-MM-DD)
     * @returns {Date} - Date object
     */
    parseDate(dateString) {
        return new Date(dateString + 'T00:00:00');
    },
    
    /**
     * Get weekday name from date
     * @param {Date|string} date - Date object or string
     * @returns {string} - Weekday name (e.g., "Monday")
     */
    getWeekdayName(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        return d.toLocaleDateString('en-US', { weekday: 'long' });
    },
    
    /**
     * Get weekday index (0 = Sunday, 1 = Monday, etc.)
     * @param {Date|string} date - Date object or string
     * @returns {number} - Weekday index
     */
    getWeekdayIndex(date) {
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        return d.getDay();
    },
    
    /**
     * Add days to a date
     * @param {Date|string} date - Date object or string (YYYY-MM-DD)
     * @param {number} days - Number of days to add (negative to subtract)
     * @returns {Date} - New date
     */
    addDays(date, days) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        d.setDate(d.getDate() + days);
        return d;
    },
    
    /**
     * Get previous day
     * @param {Date|string} date - Date object or string
     * @returns {Date} - Previous day
     */
    getPreviousDay(date) {
        return this.addDays(date, -1);
    },
    
    /**
     * Get next day
     * @param {Date|string} date - Date object or string
     * @returns {Date} - Next day
     */
    getNextDay(date) {
        return this.addDays(date, 1);
    },
    
    /**
     * Check if a date is today
     * @param {Date|string} date - Date object or string
     * @returns {boolean} - True if date is today
     */
    isToday(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        const today = this.getToday();
        return d.toDateString() === today.toDateString();
    },
    
    /**
     * Check if a date is in the past
     * @param {Date|string} date - Date object or string
     * @returns {boolean} - True if date is in the past
     */
    isPast(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        d.setHours(0, 0, 0, 0);
        const today = this.getToday();
        return d < today;
    },
    
    /**
     * Check if a date is in the future
     * @param {Date|string} date - Date object or string
     * @returns {boolean} - True if date is in the future
     */
    isFuture(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        d.setHours(0, 0, 0, 0);
        const today = this.getToday();
        return d > today;
    },
    
    /**
     * Check if a date is overdue
     * @param {Date|string} date - Date object or string
     * @returns {boolean} - True if date is overdue (in the past and not today)
     */
    isOverdue(date) {
        return this.isPast(date) && !this.isToday(date);
    },
    
    /**
     * Get date range (array of dates between start and end)
     * @param {Date|string} startDate - Start date
     * @param {Date|string} endDate - End date
     * @returns {Array<Date>} - Array of dates
     */
    getDateRange(startDate, endDate) {
        const dates = [];
        // Parse with explicit timezone handling
        let currentDate = typeof startDate === 'string' ? this._parseString(startDate) : new Date(startDate);
        const end = typeof endDate === 'string' ? this._parseString(endDate) : new Date(endDate);
        
        while (currentDate <= end) {
            dates.push(new Date(currentDate));
            currentDate.setDate(currentDate.getDate() + 1);
        }
        
        return dates;
    },
    
    /**
     * Get first day of month
     * @param {Date|string} date - Date object or string
     * @returns {Date} - First day of month
     */
    getFirstDayOfMonth(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        return new Date(d.getFullYear(), d.getMonth(), 1);
    },
    
    /**
     * Get last day of month
     * @param {Date|string} date - Date object or string
     * @returns {Date} - Last day of month
     */
    getLastDayOfMonth(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        return new Date(d.getFullYear(), d.getMonth() + 1, 0);
    },
    
    /**
     * Get month name
     * @param {Date|string} date - Date object or string
     * @returns {string} - Month name (e.g., "October")
     */
    getMonthName(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        return d.toLocaleDateString('en-US', { month: 'long' });
    },
    
    /**
     * Get year
     * @param {Date|string} date - Date object or string
     * @returns {number} - Year
     */
    getYear(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        return d.getFullYear();
    },
    
    /**
     * Get relative time string (e.g., "Today", "Yesterday", "Tomorrow", "2 days ago")
     * @param {Date|string} date - Date object or string
     * @returns {string} - Relative time string
     */
    getRelativeTime(date) {
        // Parse with explicit timezone handling
        const d = typeof date === 'string' ? this._parseString(date) : new Date(date);
        d.setHours(0, 0, 0, 0);
        const today = this.getToday();
        const diffTime = d - today;
        const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
        
        if (diffDays === 0) return 'Today';
        if (diffDays === 1) return 'Tomorrow';
        if (diffDays === -1) return 'Yesterday';
        if (diffDays > 1) return `In ${diffDays} days`;
        if (diffDays < -1) return `${Math.abs(diffDays)} days ago`;
        
        return this.formatDateShort(date);
    },
    
    /**
     * Format a timestamp with date and time
     * @param {Date|string} timestamp - ISO timestamp string or Date object (e.g., "2025-12-23T16:00:00Z")
     * @param {object} options - Intl.DateTimeFormat options
     * @returns {string} - Formatted timestamp (e.g., "Dec 27, 2025, 10:30:00 PM EST")
     */
    formatTimestamp(timestamp, options = {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        timeZoneName: 'short'
    }) {
        if (!timestamp) return 'N/A';
        try {
            // Parse with explicit timezone handling
            const d = typeof timestamp === 'string' ? this._parseString(timestamp) : new Date(timestamp);
            return d.toLocaleString('en-US', options);
        } catch (error) {
            console.error('Error formatting timestamp:', error);
            return timestamp;
        }
    }
};

// Make dateUtils globally available
window.dateUtils = dateUtils;

