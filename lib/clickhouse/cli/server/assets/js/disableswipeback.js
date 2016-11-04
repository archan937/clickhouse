/*
 * Copyright (c) 2014 Mountainstorm
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */


(function ( $ ) {
	// disable back/forward swiping
	// unfortunatly the onyl way to do this isto disable all mouse wheel
	// activity and then re-implement it ourself
	$(window).on('mousewheel', function(e) {
		// XXX might need to invert deltaY on non-macs
		var deltaX = e.originalEvent.wheelDeltaX * -1;
		var deltaY = e.originalEvent.wheelDeltaY;
		var x = Math.abs(deltaX);
		var y = Math.abs(deltaY);

		// iterate over the target and all its parents in turn
		var pathToRoot = $(e.target).add($(e.target).parents());
		$(pathToRoot.get().reverse()).each(function() {
			var el = $(this);

			if (el.css('overflow') == 'scroll') {
				// do horizontal scrolling
				if (deltaX > 0) {
					var scrollWidth = el.prop('scrollWidth');
					var scrollLeftMax = scrollWidth - el.outerWidth();
					if (el.scrollLeft() < scrollLeftMax) {
						// we can scroll right
						var	delta = scrollLeftMax - el.scrollLeft();
						if (x < delta) {
							delta = x;
						}
						x -= delta;
						el.scrollLeft(el.scrollLeft() + delta);
					}
				} else {
					if (el.scrollLeft() > 0) {
						// we can scroll left
						var delta = el.scrollLeft();
						if (x < delta) {
							delta = x;
						}
						x -= delta;
						el.scrollLeft(el.scrollLeft() - delta);
					}
				}

				// do vertical scrolling
				if (deltaY < 0) {
					var scrollHeight = el.prop('scrollHeight');
					var scrollTopMax = scrollHeight - el.outerHeight();
					if (el.scrollTop() < scrollTopMax) {
						// we can scroll down
						var	delta = scrollTopMax - el.scrollTop();
						if (y < delta) {
							delta = y;
						}
						y -= delta;
						el.scrollTop(el.scrollTop() + delta);
					}
				} else {
					if (el.scrollTop() > 0) {
						// we can scroll up
						var delta = el.scrollTop();
						if (y < delta) {
							delta = y;
						}
						y -= delta;
						el.scrollTop(el.scrollTop() - delta);
					}
				}
			}
		});
		// prevent back/forward swipe
		e.preventDefault();

	});

}( jQuery ));
