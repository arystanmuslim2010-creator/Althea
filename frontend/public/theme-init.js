(function () {
  try {
    var theme = localStorage.getItem('theme') || 'light'
    document.documentElement.setAttribute('data-theme', theme)
  } catch (error) {
    document.documentElement.setAttribute('data-theme', 'light')
  }
})()
