local metrics = require('metrics')

return {
    counter = metrics.counter,
    gauge = metrics.gauge,
    histogram = metrics.histogram
}
