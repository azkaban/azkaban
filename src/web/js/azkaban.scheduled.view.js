$.namespace('azkaban');

function removeSched(schedId) {
    var scheduleURL = contextURL + "/schedule"
    var redirectURL = contextURL + "/schedule"
    $.post(
         scheduleURL,
         {"action":"removeSched", "scheduleId":schedId},
         function(data) {
             if (data.error) {
//                 alert(data.error)
                 $('#errorMsg').text(data.error)
             }
	     else {
// 		 alert("Schedule "+schedId+" removed!")
		 window.location = redirectURL
             }
         },
         "json"
   )
}
