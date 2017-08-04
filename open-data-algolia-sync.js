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

  request({uri: 'https://data.sfgov.org/resource/8ez2-fksg.json?$limit=5000', json: true})
    .then(function(datasets) {
      var objects = []
      var object = {}
      var dataset = {}
  
      for (var i = 0, size = datasets.length; i < size; i++) {
        var d = datasets[i]
        var fieldTypes = humanFieldTypes.filter(matchFilter(d))
        var tags = typeof d.keywords !== 'undefined' ? d.keywords.split(',') : null
        
        object = {
          objectID: d.datasetid,
          systemID: d.datasetid,
          name: d.dataset_name,
          description: d.description,
          createdAt: Date.parse(d.created_date) / 1000,
          rowsUpdatedAt: Date.parse(d.last_updt_dt_data) / 1000,
          category: d.category,
          tags: tags,
          keywords: tags,
          downloads: parseInt(d.downloads, 10),
          visits: parseInt(d.visits, 10),
          publishingFrequency: d.publishing_frequency,
          publishing_dept: d.department,
          fieldTypes: fieldTypes,
          record_count: parseInt(d.record_count, 10)
        } 
        
        objects.push(object)
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