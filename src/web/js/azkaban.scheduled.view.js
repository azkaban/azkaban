$.namespace('azkaban');

function removeSched(projectId, flowName) {
    var scheduleURL = contextURL + "/schedule"
    var redirectURL = contextURL + "/schedule"
    $.post(
         scheduleURL,
         {"action":"removeSched", "projectId":projectId, "flowName":flowName},
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
