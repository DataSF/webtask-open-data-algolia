var algoliasearch = require('algoliasearch')
var request = require('request-promise')

module.exports = function(ctx, cb) {
  var client = algoliasearch(ctx.data.ALGOLIA_ID, ctx.data.ALGOLIA_SECRET)
  var index = client.initIndex(ctx.data.ALGOLIA_INDEX)
  
  var humanFieldTypes = [
    'File',
    'True/False',
    'Text',
    'Number',
    'Date/Time',
    'Geometry: Point',
    'Geometry: Line',
    'Geometry: Polygon'
  ]

  var fieldTypeMapping = {
    'File': ['blob'],
    'True/False': ['boolean'],
    'Geometry: Point': ['point', 'multipoint'],
    'Geometry: Line': ['line', 'multiline'],
    'Geometry: Polygon': ['polygon', 'multipolygon'],
    'Number': ['numeric'],
    'Date/Time': ['time', 'timestamp'],
    'Text': ['text']
  }

  function matchFilter (dataset) {
    return function(fieldType, idx, arr) {
      var lookup = fieldTypeMapping[fieldType]
      var count = 0
      for (var j = 0; j < lookup.length; j++) {
        count += parseInt(dataset[lookup[j]+'_count'],10)
      }
      return count > 0
    }
  }

  request({uri: 'https://data.sfgov.org/resource/w6q6-i3uv.json?$limit=5000', json: true})
    .then(function(datasets) {
      var objects = []
      var object = {}
      var dataset = {}
  
      for (var i = 0, size = datasets.length; i < size; i++) {
        var d = datasets[i]
        var fieldTypes = humanFieldTypes.filter(matchFilter(d))
        var keywords = typeof d.keywords !== 'undefined' ? d.keywords.split(',') : null
        
        d.objectID = d.datasetid
        d.dataId = d.nbeid
        d.id = d.datasetid
        d.publishingDepartment = d.department
        d.createdAt = Date.parse(d.created_date) / 1000
        d.rowsUpdatedAt = Date.parse(d.last_updt_dt_data) / 1000
        d.visits = parseInt(d.visits, 10)
        d.downloads = parseInt(d.downloads, 10)
        d.rowCount = parseInt(d.record_count, 10)
        d.rowLabel = d.rowlabel
        d.rowIdentifier = d.rowidentifier
        d.dataChangeFrequency = d.data_change_frequency
        d.publishingFrequency = d.publishing_frequency
        d.licenseName = "Open Data Commons Public Domain Dedication and License"
        d.licenseLink = "https://opendatacommons.org/licenses/pddl/1.0/"
        d.fieldTypes = fieldTypes
        d.keywords = keywords
        d.name = d.dataset_name
        d.systemID = d.datasetid
        
        objects.push(d)
      }
      
      index.clearIndex(function(err, content){
        if(!err) {
          index.saveObjects(objects, function(err, content){
            cb(null, content)
          })
        }
      })
    })
}