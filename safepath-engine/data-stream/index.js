
var fs = require('fs');
var vorpal = require('vorpal')();
var drivingStyle = "safe";

vorpal
  .command('safe', 'Outputs "Simulating Safe Driving mode".')
  .action(function(args, callback) {
  	drivingStyle = "safe";
    callback();
  });

vorpal
  .command('rash', 'Outputs "Simulating Rash Driving mode".')
  .action(function(args, callback) {
  	drivingStyle = "rash";
    callback();
  });

vorpal
  .command('mix', 'Outputs "Simulating MIx Driving mode".')
  .action(function(args, callback) {
    drivingStyle = "mix";
    callback();
  });


vorpal
  .delimiter('drivingSimulator$')
  .show();


var geoData = ["12.9081,77.6476",
"12.9279,77.6271",
"12.4877,77.3579",
"12.9385,77.6308",
"12.9611,77.6472",
"12.9698,77.7499",
"13.0358,77.5970",
"12.9719,77.6412",
"13.0159,77.6379",
"12.8168,77.6989",
"12.9857,77.6057",
"13.0040,77.6878",
"12.9767,77.5713"];

var fileNameCounter = 1;


var dataWriterForTimeWindow = function () {
	var filename  = "data/dataset_"+fileNameCounter+".csv";
	console.log("generating "+filename);
	var wstream = fs.createWriteStream(filename);
	for(var i=0; i<7; i++) {
		wstream.write(drivingData(drivingStyle));
	}
    wstream.end();
    console.log(filename+" generated");
    console.log('--------------------');
    fileNameCounter++;
    return;
}

var drivingData = function (quality="mix") {

	console.log("DRIVING STYLE = ", quality);

    switch (quality){
    	//real world driving with mixed events
    	case "mix":
		    var driverId = function (min=1,max=5) {
		    	min = Math.ceil(min);
				max = Math.floor(max);
		     	return Math.floor(Math.random() * (max - min)) + min;
		    }();

		    var coords =function (min=0,max=12) {
		    	min = Math.ceil(min);
				max = Math.floor(max);
		     	return geoData[Math.floor(Math.random() * (max - min)) + min];

		    }();
		    var pitch = function (min= -1.34,max=2.6) {

		     	return (Math.random() * (max - min) + min).toFixed(3);
		    }();
		    var roll = function (min= -1.7,max=1.7) {

		     	return (Math.random() * (max - min) + min).toFixed(3);
		    }();
		    var acc_x = function (min= -3.5,max= 3.5) {
		     	return (Math.random() * (max - min) + min).toFixed(3);
		    }();
		    var acc_y = function (min= -4.5,max=1.55) {
		     	return (Math.random() * (max - min) + min).toFixed(3);
		    }();
		    return (driverId+","+coords+","+acc_x+","+acc_y+","+pitch+","+roll+"\n");
    	break;

//simulated safe driving
    	case "safe":
		    var driverId = function (min=1,max=5) {
		    	min = Math.ceil(min);
				max = Math.floor(max);
		     	return Math.floor(Math.random() * (max - min)) + min;
		    }();

		    var coords =function (min=0,max=12) {
		    	min = Math.ceil(min);
				max = Math.floor(max);
		     	return geoData[Math.floor(Math.random() * (max - min)) + min];

		    }();
		    var pitch = function (min= -0.14,max=0.25) {
		     	return (Math.random() * (max - min) + min).toFixed(3);
		    }();
		    var roll = function (min= -0.3,max=0.3) {

		     	return (Math.random() * (max - min) + min).toFixed(3);
		    }();
		    var acc_x = function (min= -1.503,max= 1.5) {
		     	return (Math.random() * (max - min) + min).toFixed(3);
		    }();
		    var acc_y = function (min= -1.011,max=0.249) {
		     	return (Math.random() * (max - min) + min).toFixed(3);
		    }();

		    return (driverId+","+coords+","+acc_x+","+acc_y+","+pitch+","+roll+"\n");
    	break;

//simulated rash driving
    	default:

		    var driverId = function (min=1,max=5) {
		    	min = Math.ceil(min);
				max = Math.floor(max);
		     	return Math.floor(Math.random() * (max - min)) + min;
		    }();

		    var coords =function (min=0,max=12) {
		    	min = Math.ceil(min);
				max = Math.floor(max);
		     	return geoData[Math.floor(Math.random() * (max - min)) + min];

		    }();
		    var pitch = function (min= -0.14,max=0.25) {

		    	var min1 = -1,
		    	max1=-0.2,
		    	min2=0.26,
		    	max2=2;

		    	if(Math.round(Math.random() + 1) ==1) {
		    		return (Math.random() * (max1 - min1) + min1).toFixed(3);
		    	}
		    	else {
		    		return (Math.random() * (max2 - min2) + min2).toFixed(3);
		    	}


		    }();
		    var roll = function (min= -0.3,max=0.3) {

		   		var min1 = -3,
		    	max1=-0.3,
		    	min2=0.3,
		    	max2=2;

		     	if(Math.round(Math.random() + 1) ==1) {
		    		return (Math.random() * (max1 - min1) + min1).toFixed(3);
		    	}
		    	else {
		    		return (Math.random() * (max2 - min2) + min2).toFixed(3);
		    	}
		    }();
		    var acc_x = function (min= -1.5,max= 1.5) {

		   		var min1 = -3.5,
		    	max1=-1.5,
		    	min2=1.5,
		    	max2=3.5;

		     	if(Math.round(Math.random() + 1) ==1) {
		    		return (Math.random() * (max1 - min1) + min1).toFixed(3);
		    	}
		    	else {
		    		return (Math.random() * (max2 - min2) + min2).toFixed(3);
		    	}
		    }();
		    var acc_y = function (min= -1.0,max=0.25) {

		   		var min1 = -3.0,
		    	max1=-1.0,
		    	min2=0.3,
		    	max2=2.5;

		     	if(Math.round(Math.random() + 1) ==1) {
		    		return (Math.random() * (max1 - min1) + min1).toFixed(3);
		    	}
		    	else {
		    		return (Math.random() * (max2 - min2) + min2).toFixed(3);
		    	}
		    }();
		    return (driverId+","+coords+","+acc_x+","+acc_y+","+pitch+","+roll+"\n");
    }
    return ;

}

setInterval(dataWriterForTimeWindow, 8000);
