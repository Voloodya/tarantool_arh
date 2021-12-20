var searchBoxIsOpen = false;
var searchBoxMinIsOpen = false;

$('.search_drop_btn.max').on('click touch', function(){
    searchBoxIsOpen = !searchBoxIsOpen;
    if (searchBoxIsOpen) {
        $(this).addClass('active');
        $('.seaarch_drop_input.max').css('display', 'block');
        $('.seaarch_drop_input.max input[name="q"]').focus();
    } else {
        $(this).removeClass('active');
        $('.seaarch_drop_input.max').css('display', 'none');
    }
});

$('.wy-nav-top .search_drop_btn').on('click touch', function(){
    searchBoxMinIsOpen = !searchBoxMinIsOpen;
    if (searchBoxMinIsOpen) {
        $(this).addClass('active');
        $('.wy-nav-top .seaarch_drop_input').css('display', 'block');
        $('.wy-nav-top .seaarch_drop_input input[name="q"]').focus();
    } else {
        $(this).removeClass('active');
        $('.wy-nav-top .seaarch_drop_input').css('display', 'none');
    }
});