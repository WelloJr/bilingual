document.getElementById('language-switcher').addEventListener('change', function() {
    var selectedLang = this.value;
    if (selectedLang === 'en') {
        document.documentElement.setAttribute('lang', 'en');
        document.documentElement.setAttribute('dir', 'ltr');
        document.getElementById('content-en').style.display = 'block';
        document.getElementById('content-ar').style.display = 'none';
    } else if (selectedLang === 'ar') {
        document.documentElement.setAttribute('lang', 'ar');
        document.documentElement.setAttribute('dir', 'rtl');
        document.getElementById('content-en').style.display = 'none';
        document.getElementById('content-ar').style.display = 'block';
    }
});