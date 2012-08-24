$(document).ready(function() {

    $.template("consumer-list",
	       "<li class=\"consumer-link\" " +
	       "rel=\"/consumers/${id}\"" +
	       "id=\"consumer-link-${id}\">" +
	       "${email}</li>");

    var consumerUnpacker = function(data) {
	$("#update-consumer").html($("#consumer-input-tmpl").tmpl({
	    id: data["id"],
	    drain_url: data["drain_url"],
	    email: data["email"],
	    token: data["token"]
	}))
    };

    var loadConsumers = function() {
	$.ajax({
	    url: "/consumers",
	    dataType: "json",
	    type: "GET",
	    success: function(data) {
		$("#consumer-list-spin").hide();
		$.each(data, function(i, d) {
		    var k = "consumer-link-" + String(d["id"]);
		    if ($("#" + k).length == 0) {
			$.tmpl("consumer-list", d).appendTo("#consumer-list")
		    } else {
			$("#" + k).html($.tmpl("consumer-list", d));
		    }
		});
	    }
	});
    };

    var loadConsumer = function(url) {
	$.ajax({
	    url: url,
	    dataType: "json",
	    type: "GET",
	    success: function(data) {consumerUnpacker(data)}
	});
    };

    $(".consumer-link").live("click", function() {
	$(".selected").removeClass("selected");
	$(this).addClass("selected")
	loadConsumer($(this).attr('rel'));
    });

    $("#update-consumer").live("submit", function() {
	$.ajax({
	    url: "/consumers",
	    type: "PUT",
	    data: {email: $(this).find("#email").val(),
		   token: $(this).find("#token").val(),
		   id: $(this).find("#id").val()},
	    beforeSend: function() {
		$("#update-consumer button").attr("disabled", true);
		$("#update-consumer button").text("processing...");
	    },
	    success: function(d) {
		consumerUnpacker(d);
		loadConsumers();
		$("#update-consumer button").attr("disabled", false);
		$("#update-consumer button").text("put");
	    }
	});
	return false;
    });

    $("#new-consumer").live("click", function() {
	$(".selected").removeClass("selected");
	$(this).addClass("selected");
	consumerUnpacker({id: ""})
    });

    loadConsumers();
});
