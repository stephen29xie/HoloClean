$(function() {

//    state variables to track user progress
    var post_params = 0;
    var post_data = 0;
    var post_dc = 0;
    var post_error_detectors = 0;
    var post_repair = 0;

    $('#navigation-menu').onePageNav();

    $body = $("body");

    var datatable_attributes;

//    Submit parameters to initialize holoclean object and session object
    $('#submitparam').on('click', function() {

        if (post_params == 1) {
            alert('Already Submitted HoloClean Parameters');
            return;
        }

        $body.addClass("loading");

        // objectifyForm takes a serialized array form and returns it as a cleaner json object
        function objectifyForm(formArray) {
              var returnArray = {};
              for (var i = 0; i < formArray.length; i++){
                returnArray[formArray[i]['name']] = Number(formArray[i]['value']);
              }
              return returnArray;
        }

        var jsonform = JSON.stringify(objectifyForm($("#paramform").serializeArray()))  // returns a string

        $.ajax({
            type: 'POST',
            contentType: 'application/json',
            url: '/submitholoparams',
            data: jsonform,
            success: function() {
                post_params = 1;
                $('#submitparam-check').css('visibility', 'visible');
                $body.removeClass("loading");
                $('html, body').animate({
                    scrollTop: $("#file-div").offset().top
                }, 1000);
            },
            error: function(jqXHR, textStatus, errorThrown) {alert(jqXHR, textStatus, errorThrown)}
        })
    })

//    Submit paths for data and denial constraint files
    $('#submitpath').on('click',function() {

        if (post_data == 1 || post_dc == 1) {
            alert('Already Submitted Files');
            return;
        }

        $body.addClass("loading");

        var datapath = $('#filepath').val();
        var dcpath = $('#dcpath').val();

        $.ajax({
            type: 'POST',
            url: '/loadfilepaths',
            contentType: 'application/json',
            data: JSON.stringify({
                'filepath': datapath,
                'dcpath' : dcpath
            }),
            success: function () {

                post_data = 1;
                post_dc = 1;

                $('#submitpath-check').css('visibility', 'visible');

                $.ajax ({
                    'type': 'GET',
                    'url': '/getAttributes',
                    'success': function(attributes) {
                        datatable_attributes = attributes
                        $body.removeClass("loading");
                        $('html, body').animate({
                            scrollTop: $("#errordetector-div").offset().top
                        }, 1000);
                    }
                })

            },
            error: function(jqXHR, textStatus, errorThrown) {alert(jqXHR, textStatus, errorThrown)},
        })
    });


//    Submit error detectors
    $('#submiterrordetectors').on('click', function() {

        if (post_error_detectors == 1) {
            alert('Already Submitted Error Detectors');
            return;
        }

        $body.addClass("loading");

        var nullerrordetectorflag = 0;
        var dcerrordetectorflag = 0;

        if ($('#nullerrordetector').is(':checked')) {nullerrordetectorflag = 1}
        if ($('#dcerrordetector').is(':checked')) {dcerrordetectorflag = 1}

        $.ajax({
            type: 'POST',
            url: '/submiterrordetectors',
            contentType: 'application/json',
            data: JSON.stringify(
                {
                    'nullerrordetectorflag' : nullerrordetectorflag,
                    'dcerrordetectorflag' : dcerrordetectorflag
                }),
            success: function () {
                post_error_detectors = 1;
                $('#submiterrordetectors-check').css('visibility','visible');
                $body.removeClass("loading");
                $('html, body').animate({
                    scrollTop: $("#repair-div").offset().top
                }, 1000);
            },
            error: function(jqXHR, textStatus, errorThrown) {alert(jqXHR, textStatus, errorThrown)}
        })
    })


//    Repair
    $('#repair').on('click', function() {

        if (post_repair == 1) {
            alert('Already Repaired');
            return;
        }

        $body.addClass("loading");

        var initfeaturizer_flag = 0;
        var dcfeaturizer_flag = 0;
        var cooccurfeaturizer_flag = 0;

        if ($('#initfeaturizer').is(':checked')) {initfeaturizer_flag = 1}
        if ($('#dcfeaturizer').is(':checked')) {dcfeaturizer_flag = 1}
        if ($('#cooccurfeaturizer').is(':checked')) {cooccurfeaturizer_flag = 1}

        $.ajax({
            type: 'POST',
            url: '/repair',
            contentType: 'application/json',
            data: JSON.stringify(
                {
                    'initfeaturizer_flag': initfeaturizer_flag,
                    'dcfeaturizer_flag': dcfeaturizer_flag,
                    'cooccurfeaturizer_flag': cooccurfeaturizer_flag
                }),
            error: function(jqXHR, textStatus, errorThrown) {alert(jqXHR, textStatus, errorThrown)},
            success: function() {

                post_repair = 1;

                $('#repair-check').css('visibility','visible');

                var table = $('#home-repaired-table').DataTable({
                    scrollX: true,
                    scrollY: true,
                    processing: true,
                    select: true,
                    ajax: {
                            url: '/getrepairedJSON',
                            type: 'GET',
                            dataSrc: '',
                            error: function(jqXHR, textStatus, errorThrown) {alert(jqXHR, textStatus, errorThrown)}
                        },
                    columns: datatable_attributes
                })

                //    Interactive Functionality to click on the rendered DataTable
                $('#home-repaired-table tbody').on( 'click', 'td', function () {

                    var cur_value = table.cell( this ).data();

                    var row_clicked = $(this).closest('tr');
                    var ind = table.row(row_clicked).data()['__ind'];

                    var idx = table.cell( this ).index().column;
                    var title = table.column( idx ).header();
                    var attribute = $(title).html();

                    var initvalue;
                    var inferredvalues;

                    $.ajax({
                        type: 'POST',
                        url: '/getInitValue',
                        contentType: 'application/JSON',
                        data: JSON.stringify({
                            'attribute': attribute,
                            '__ind': ind
                        }),
                        success: function(response) {
                            initvalue = response
                            alert('__ind: ' + ind + '\n' + 'Attribute: ' + attribute + '\n' +
                                  'Current Value: ' + cur_value + '\n' + 'Init Value: ' + initvalue)

                            $.ajax({
                                type: 'POST',
                                url: '/getInferredValues',
                                contentType: 'application/JSON',
                                data: JSON.stringify({
                                    'attribute': attribute,
                                    '__ind': ind
                                }),
                                success: function(response) {
                                    inferredvalues = response
                                    alert('__ind: ' + ind + '\n' + 'Attribute: ' + attribute + '\n' +
                                          'Inferred Value(s): \n' + response)
                                }
                            })

                        }

                    })
                });

                $body.removeClass("loading");

                $('html, body').animate({
                    scrollTop: $("#evaluation-div").offset().top
                }, 1000);
            }
        })
    })
});
