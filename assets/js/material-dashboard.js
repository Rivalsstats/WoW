"use strict";


// initialization of Tooltips
var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'))
var tooltipList = tooltipTriggerList.map(function(tooltipTriggerEl) {
  return new bootstrap.Tooltip(tooltipTriggerEl)
})

// when input is focused add focused class for style
function focused(el) {
  if (el.parentElement.classList.contains('input-group')) {
    el.parentElement.classList.add('focused');
  }
}

// when input is focused remove focused class for style
function defocused(el) {
  if (el.parentElement.classList.contains('input-group')) {
    el.parentElement.classList.remove('focused');
  }
}

// helper for adding on all elements multiple attributes
function setAttributes(el, options) {
  Object.keys(options).forEach(function(attr) {
    el.setAttribute(attr, options[attr]);
  })
}

// adding on inputs attributes for calling the focused and defocused functions
if (document.querySelectorAll('.input-group').length != 0) {
  var allInputs = document.querySelectorAll('input.form-control');
  allInputs.forEach(el => setAttributes(el, {
    "onfocus": "focused(this)",
    "onfocusout": "defocused(this)"
  }));
}


// Fixed Plugin

if (document.querySelector('.fixed-plugin')) {
  var fixedPlugin = document.querySelector('.fixed-plugin');
  var fixedPlugin = document.querySelector('.fixed-plugin');
  var fixedPluginButton = document.querySelector('.fixed-plugin-button');
  var fixedPluginButtonNav = document.querySelector('.fixed-plugin-button-nav');
  var fixedPluginCard = document.querySelector('.fixed-plugin .card');
  var fixedPluginCloseButton = document.querySelectorAll('.fixed-plugin-close-button');

  if (fixedPluginButton) {
    fixedPluginButton.onclick = function() {
      if (!fixedPlugin.classList.contains('show')) {
        fixedPlugin.classList.add('show');
      } else {
        fixedPlugin.classList.remove('show');
      }
    }
  }

  if (fixedPluginButtonNav) {
    fixedPluginButtonNav.onclick = function() {
      if (!fixedPlugin.classList.contains('show')) {
        fixedPlugin.classList.add('show');
      } else {
        fixedPlugin.classList.remove('show');
      }
    }
  }

  fixedPluginCloseButton.forEach(function(el) {
    el.onclick = function() {
      fixedPlugin.classList.remove('show');
    }
  })

  document.querySelector('body').onclick = function(e) {
    if (e.target != fixedPluginButton && e.target != fixedPluginButtonNav && e.target.closest('.fixed-plugin .card') != fixedPluginCard) {
      fixedPlugin.classList.remove('show');
    }
  }

}

//Set Sidebar Color
function sidebarColor(a) {
  var parent = document.querySelector(".nav-link.active");
  var color = a.getAttribute("data-color");

  if(!parent) return;

  if (parent.classList.contains('bg-gradient-primary')) {
    parent.classList.remove('bg-gradient-primary');
  }
  if (parent.classList.contains('bg-gradient-dark')) {
    parent.classList.remove('bg-gradient-dark');
  }
  if (parent.classList.contains('bg-gradient-info')) {
    parent.classList.remove('bg-gradient-info');
  }
  if (parent.classList.contains('bg-gradient-success')) {
    parent.classList.remove('bg-gradient-success');
  }
  if (parent.classList.contains('bg-gradient-warning')) {
    parent.classList.remove('bg-gradient-warning');
  }
  if (parent.classList.contains('bg-gradient-danger')) {
    parent.classList.remove('bg-gradient-danger');
  }
  parent.classList.add('bg-gradient-' + color);
}

// Set Sidebar Type
function sidebarType(a) {
  var parent = a.parentElement.children;
  var color = a.getAttribute("data-class");
  var body = document.querySelector("body");
  var bodyWhite = document.querySelector("body:not(.dark-version)");
  var bodyDark = body.getAttribute("data-bs-theme") && body.getAttribute("data-bs-theme") === "dark";

  var colors = [];

  for (var i = 0; i < parent.length; i++) {
    parent[i].classList.remove('active');
    colors.push(parent[i].getAttribute('data-class'));
  }

  if (!a.classList.contains('active')) {
    a.classList.add('active');
  } else {
    a.classList.remove('active');
  }

  var sidebar = document.querySelector('.sidenav');

  for (var i = 0; i < colors.length; i++) {
    sidebar.classList.remove(colors[i]);
  }

  sidebar.classList.add(color);


  // Remove text-white/text-dark classes
  if (color == 'bg-transparent' || color == 'bg-white') {
    var textWhites = document.querySelectorAll('.sidenav .text-white:not(.nav-link-text):not(.active)');
    for (let i = 0; i < textWhites.length; i++) {
      textWhites[i].classList.remove('text-white');
      textWhites[i].classList.add('text-dark');
    }
  } else {
    var textDarks = document.querySelectorAll('.sidenav .text-dark');
    for (let i = 0; i < textDarks.length; i++) {
      textDarks[i].classList.add('text-white');
      textDarks[i].classList.remove('text-dark');
    }
  }

  if (color == 'bg-transparent' && bodyDark) {
    var textDarks = document.querySelectorAll('.navbar-brand .text-dark');
    for (let i = 0; i < textDarks.length; i++) {
      textDarks[i].classList.add('text-white');
      textDarks[i].classList.remove('text-dark');
    }
  }

  // Remove logo-white/logo-dark

  if ((color == 'bg-transparent' || color == 'bg-white') && bodyWhite) {
    var navbarBrand = document.querySelector('.navbar-brand-img');
    var navbarBrandImg = navbarBrand.src;

    if (navbarBrandImg.includes('logo-ct.png')) {
      var navbarBrandImgNew = navbarBrandImg.replace("logo-ct", "logo-ct-dark");
      navbarBrand.src = navbarBrandImgNew;
    }
  } else {
    var navbarBrand = document.querySelector('.navbar-brand-img');
    var navbarBrandImg = navbarBrand.src;
    if (navbarBrandImg.includes('logo-ct-dark.png')) {
      var navbarBrandImgNew = navbarBrandImg.replace("logo-ct-dark", "logo-ct");
      navbarBrand.src = navbarBrandImgNew;
    }
  }

  if (color == 'bg-white' && bodyDark) {
    var navbarBrand = document.querySelector('.navbar-brand-img');
    var navbarBrandImg = navbarBrand.src;

    if (navbarBrandImg.includes('logo-ct.png')) {
      var navbarBrandImgNew = navbarBrandImg.replace("logo-ct", "logo-ct-dark");
      navbarBrand.src = navbarBrandImgNew;
    }
  }
}


// Debounce Function
// Returns a function, that, as long as it continues to be invoked, will not
// be triggered. The function will be called after it stops being called for
// N milliseconds. If `immediate` is passed, trigger the function on the
// leading edge, instead of the trailing.
function debounce(func, wait, immediate) {
  var timeout;
  return function() {
    var context = this,
      args = arguments;
    var later = function() {
      timeout = null;
      if (!immediate) func.apply(context, args);
    };
    var callNow = immediate && !timeout;
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
    if (callNow) func.apply(context, args);
  };
};

// initialization of Toasts
document.addEventListener("DOMContentLoaded", function() {
  var toastElList = [].slice.call(document.querySelectorAll(".toast"));

  var toastList = toastElList.map(function(toastEl) {
    return new bootstrap.Toast(toastEl);
  });

  var toastButtonList = [].slice.call(document.querySelectorAll(".toast-btn"));

  toastButtonList.map(function(toastButtonEl) {
    toastButtonEl.addEventListener("click", function() {
      var toastToTrigger = document.getElementById(toastButtonEl.dataset.target);

      if (toastToTrigger) {
        var toast = bootstrap.Toast.getInstance(toastToTrigger);
        toast.show();
      }
    });
  });
});

// Tabs navigation

var total = document.querySelectorAll('.nav-pills');

function initNavs() {
  total.forEach(function(item, i) {
    var moving_div = document.createElement('div');
    var first_li = item.querySelector('li:first-child .nav-link');
    var tab = first_li.cloneNode();
    tab.innerHTML = "-";

    moving_div.classList.add('moving-tab', 'position-absolute', 'nav-link');
    moving_div.appendChild(tab);
    item.appendChild(moving_div);

    var list_length = item.getElementsByTagName("li").length;

    moving_div.style.padding = '0px';
    moving_div.style.width = item.querySelector('li:nth-child(1)').offsetWidth + 'px';
    moving_div.style.transform = 'translate3d(0px, 0px, 0px)';
    moving_div.style.transition = '.5s ease';

    item.onmouseover = function(event) {
      let target = getEventTarget(event);
      let li = target.closest('li'); // get reference
      if (li) {
        let nodes = Array.from(li.closest('ul').children); // get array
        let index = nodes.indexOf(li) + 1;
        item.querySelector('li:nth-child(' + index + ') .nav-link').onclick = function() {
          moving_div = item.querySelector('.moving-tab');
          let sum = 0;
          if (item.classList.contains('flex-column')) {
            for (var j = 1; j <= nodes.indexOf(li); j++) {
              sum += item.querySelector('li:nth-child(' + j + ')').offsetHeight;
            }
            moving_div.style.transform = 'translate3d(0px,' + sum + 'px, 0px)';
            moving_div.style.height = item.querySelector('li:nth-child(' + j + ')').offsetHeight;
          } else {
            for (var j = 1; j <= nodes.indexOf(li); j++) {
              sum += item.querySelector('li:nth-child(' + j + ')').offsetWidth;
            }
            moving_div.style.transform = 'translate3d(' + sum + 'px, 0px, 0px)';
            moving_div.style.width = item.querySelector('li:nth-child(' + index + ')').offsetWidth + 'px';
          }
        }
      }
    }
  });
}

setTimeout(function() {
  initNavs();
}, 100);

// Tabs navigation resize

window.addEventListener('resize', function(event) {
  total.forEach(function(item, i) {
    item.querySelector('.moving-tab').remove();
    var moving_div = document.createElement('div');
    var tab = item.querySelector(".nav-link.active").cloneNode();
    tab.innerHTML = "-";

    moving_div.classList.add('moving-tab', 'position-absolute', 'nav-link');
    moving_div.appendChild(tab);

    item.appendChild(moving_div);

    moving_div.style.padding = '0px';
    moving_div.style.transition = '.5s ease';

    let li = item.querySelector(".nav-link.active").parentElement;

    if (li) {
      let nodes = Array.from(li.closest('ul').children); // get array
      let index = nodes.indexOf(li) + 1;

      let sum = 0;
      if (item.classList.contains('flex-column')) {
        for (var j = 1; j <= nodes.indexOf(li); j++) {
          sum += item.querySelector('li:nth-child(' + j + ')').offsetHeight;
        }
        moving_div.style.transform = 'translate3d(0px,' + sum + 'px, 0px)';
        moving_div.style.width = item.querySelector('li:nth-child(' + index + ')').offsetWidth + 'px';
        moving_div.style.height = item.querySelector('li:nth-child(' + j + ')').offsetHeight;
      } else {
        for (var j = 1; j <= nodes.indexOf(li); j++) {
          sum += item.querySelector('li:nth-child(' + j + ')').offsetWidth;
        }
        moving_div.style.transform = 'translate3d(' + sum + 'px, 0px, 0px)';
        moving_div.style.width = item.querySelector('li:nth-child(' + index + ')').offsetWidth + 'px';

      }
    }
  });

  if (window.innerWidth < 991) {
    total.forEach(function(item, i) {
      if (!item.classList.contains('flex-column')) {
        item.classList.remove('flex-row');
        item.classList.add('flex-column', 'on-resize');
        let li = item.querySelector(".nav-link.active").parentElement;
        let nodes = Array.from(li.closest('ul').children); // get array
        let index = nodes.indexOf(li) + 1;
        let sum = 0;
        for (var j = 1; j <= nodes.indexOf(li); j++) {
          sum += item.querySelector('li:nth-child(' + j + ')').offsetHeight;
        }
        var moving_div = document.querySelector('.moving-tab');
        moving_div.style.width = item.querySelector('li:nth-child(1)').offsetWidth + 'px';
        moving_div.style.transform = 'translate3d(0px,' + sum + 'px, 0px)';

      }
    });
  } else {
    total.forEach(function(item, i) {
      if (item.classList.contains('on-resize')) {
        item.classList.remove('flex-column', 'on-resize');
        item.classList.add('flex-row');
        let li = item.querySelector(".nav-link.active").parentElement;
        let nodes = Array.from(li.closest('ul').children); // get array
        let index = nodes.indexOf(li) + 1;
        let sum = 0;
        for (var j = 1; j <= nodes.indexOf(li); j++) {
          sum += item.querySelector('li:nth-child(' + j + ')').offsetWidth;
        }
        var moving_div = document.querySelector('.moving-tab');
        moving_div.style.transform = 'translate3d(' + sum + 'px, 0px, 0px)';
        moving_div.style.width = item.querySelector('li:nth-child(' + index + ')').offsetWidth + 'px';
      }
    })
  }
});

// Function to remove flex row on mobile devices
if (window.innerWidth < 991) {
  total.forEach(function(item, i) {
    if (item.classList.contains('flex-row')) {
      item.classList.remove('flex-row');
      item.classList.add('flex-column', 'on-resize');
    }
  });
}

function getEventTarget(e) {
  e = e || window.event;
  return e.target || e.srcElement;
}

// End tabs navigation

window.onload = function() {
  // Material Design Input function
  var inputs = document.querySelectorAll('input');

  for (var i = 0; i < inputs.length; i++) {
    inputs[i].addEventListener('focus', function(e) {
      this.parentElement.classList.add('is-focused');
    }, false);

    inputs[i].onkeyup = function(e) {
      if (this.value != "") {
        this.parentElement.classList.add('is-filled');
      } else {
        this.parentElement.classList.remove('is-filled');
      }
    };

    inputs[i].addEventListener('focusout', function(e) {
      if (this.value != "") {
        this.parentElement.classList.add('is-filled');
      }
      this.parentElement.classList.remove('is-focused');
    }, false);
  }

  // Ripple Effect
  var ripples = document.querySelectorAll('.btn');

  for (var i = 0; i < ripples.length; i++) {
    ripples[i].addEventListener('click', function(e) {
      var targetEl = e.target;
      var rippleDiv = targetEl.querySelector('.ripple');

      rippleDiv = document.createElement('span');
      rippleDiv.classList.add('ripple');
      rippleDiv.style.width = rippleDiv.style.height = Math.max(targetEl.offsetWidth, targetEl.offsetHeight) + 'px';
      targetEl.appendChild(rippleDiv);

      rippleDiv.style.left = (e.offsetX - rippleDiv.offsetWidth / 2) + 'px';
      rippleDiv.style.top = (e.offsetY - rippleDiv.offsetHeight / 2) + 'px';
      rippleDiv.classList.add('ripple');
      setTimeout(function() {
        // Guard against the ripple's host being detached/replaced before the
        // timer fires (e.g. a button that rewrites its inner content on click).
        if (rippleDiv.parentElement) {
          rippleDiv.parentElement.removeChild(rippleDiv);
        }
      }, 600);
    }, false);
  }
};

// Toggle Sidenav
const iconNavbarSidenav = document.getElementById('iconNavbarSidenav');
const iconSidenav = document.getElementById('iconSidenav');
const sidenav = document.getElementById('sidenav-main');
let body = document.getElementsByTagName('body')[0];
let className = 'g-sidenav-pinned';

if (iconNavbarSidenav) {
  iconNavbarSidenav.addEventListener("click", toggleSidenav);
}

if (iconSidenav) {
  iconSidenav.addEventListener("click", toggleSidenav);
}

function toggleSidenav() {
  if (body.classList.contains(className)) {
    body.classList.remove(className);
    setTimeout(function() {
      sidenav.classList.remove('bg-body');
    }, 100);
    sidenav.classList.remove('bg-transparent');

  } else {
    body.classList.add(className);
    sidenav.classList.add('bg-body');
    sidenav.classList.remove('bg-transparent');
    iconSidenav.classList.remove('d-none');
  }
}


// Deactivate sidenav type buttons on resize and small screens
window.addEventListener("resize", sidenavTypeOnResize);
window.addEventListener("load", sidenavTypeOnResize);

function sidenavTypeOnResize() {
  let elements = document.querySelectorAll('[onclick="sidebarType(this)"]');
  if (window.innerWidth < 1200) {
    elements.forEach(function(el) {
      el.classList.add('disabled');
    });
  } else {
    elements.forEach(function(el) {
      el.classList.remove('disabled');
    });
  }
}

// Light Mode / Dark Mode
  const checkbox = document.getElementById('dark-version');
  const selectors = '.klaro .context-notice, .klaro .cookie-notice, .klaro .cookie-modal, #klaro';
  // apply theme to the document body
  function applyTheme(theme) {
    document.body.setAttribute('data-bs-theme', theme);
    document.querySelectorAll(selectors).forEach(el => {
        if (theme === 'dark') el.classList.add('cm-dark');
        else el.classList.remove('cm-dark');
    });
  }

  // set checkbox boolean property (NOT attribute)
  function setCheckboxForTheme(theme) {
    checkbox.checked = (theme === 'dark');
  }
  
  // run on load: pick stored theme, else system preference, else light
  window.addEventListener('DOMContentLoaded', () => {
    const stored = localStorage.getItem('theme');
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    const theme = stored || (prefersDark ? 'dark' : 'light');

    applyTheme(theme);
    setCheckboxForTheme(theme);
  });

  // persist and toggle when user changes the switch
  checkbox.addEventListener('change', (e) => {
    const theme = e.target.checked ? 'dark' : 'light';
    applyTheme(theme);
    localStorage.setItem('theme', theme);
  });


// side bullets

const indicators = document.querySelectorAll(".indicator");
const sections = document.querySelectorAll("section");

if (indicators) {
  const resetCurrentActiveIndicator = () => {
    const activeIndicator = document.querySelector(".indicator.active");
    if (activeIndicator) {
      activeIndicator.classList.remove("active");
    }
  };

  const onSectionLeavesViewport = (section) => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            resetCurrentActiveIndicator();
            const element = entry.target;
            const indicator = document.querySelector(`a[href='#${element.id}']`);
            if (indicator) {
              indicator.classList.add("active");
            }
            return;
          }
        });
      }, {
        root: null,
        rootMargin: "0px",
        threshold: 0.75
      }
    );
    observer.observe(section);
  };

  indicators.forEach((indicator) => {
    indicator.addEventListener("click", function(event) {
      event.preventDefault();
      document
        .querySelector(this.getAttribute("href"))
        .scrollIntoView({
          behavior: "smooth"
        });
      resetCurrentActiveIndicator();
      this.classList.add("active");
    });
  });

  sections.forEach(onSectionLeavesViewport);
}