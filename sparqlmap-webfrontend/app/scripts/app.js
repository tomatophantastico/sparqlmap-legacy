'use strict';

angular.module('sparqlmapWebfrontendApp', [
  'ngCookies',
  'ngResource',
  'ngSanitize',
  'ngRoute'
])
  .config(function ($routeProvider) {
    $routeProvider
      .when('/', {
        templateUrl: 'views/welcome.html',
        controller: 'MainCtrl'
      })
      .when('/status',{
        templateUrl: 'views/status.html',
        controller: 'StatusCtrl'
      })
      .otherwise({
        redirectTo: '/'
      });
  });
