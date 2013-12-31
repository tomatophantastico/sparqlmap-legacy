'use strict';

angular.module('sparqlmapWebfrontendApp')
  .controller('MainCtrl', function ($scope) {
    $scope.awesomeThings = [
      'HTML5 Boilerplate',
      'AngularJS',
      'Karma'
    ];
  })
  .controller('StatusCtrl', function($scope){
  	$scope.status = {
  		'DEFAULT' : {
        'context' : 'DEFAULT', 
        'errors': [],
        'requests': 2345 }
  	};
  })
  .controller('HeaderCtrl', function($scope,$location){
     $scope.isActive = function (viewLocation) { 
        return viewLocation === $location.path();
    };
  });
  