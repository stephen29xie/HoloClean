$(function() {


    $('#submitpath').on('click',function() {

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
            success: function () {alert('success')},
            error : function () {alert('error')}
        })
    });

    $('#submitparam').on('click', function() {
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
            success: function() {alert('success')},
            error: function() {alert('error')}
        })
    })


    $('#submiterrordetectors').on('click', function() {

        var nullerrordetectorflag = 0
        var dcerrordetectorflag = 0

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
            success: function () {alert('success')},
            error: function () {alert('error')}
        })
    })

    $('#repair').on('click', function() {
        $.ajax({
            type: 'POST',
            url: '/repair',
            success: function() {alert('success')},
            error: function() {alert('error')}
        })
    })
});
