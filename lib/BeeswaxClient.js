'use strict';

var urlUtils = require('url'),
    util = require('util'),
    Promise = require('bluebird'),
    request = require('request'),
    rp = require('request-promise'),
    rpErrors = require('request-promise/errors');

require('ssl-root-cas').inject();

// Return true if value is a Plain Old Javascript Object
function isPOJO(value) {
    return !!(value && value.constructor === Object);
}

// Upon instantiation, will setup objects with bound CRUD methods for each entry here
var entities = {
    advertisers: {
        endpoint: '/rest/advertiser',
        idField: 'advertiser_id'
    },
    campaigns: {
        endpoint: '/rest/campaign',
        idField: 'campaign_id'
    },
    creatives: {
        endpoint: '/rest/creative',
        idField: 'creative_id'
    },
    lineItems: {
        endpoint: '/rest/line_item',
        idField: 'line_item_id'
    },
    lineItemFlights: {
        endpoint: '/rest/line_item_flight',
        idField: 'line_item_flight_id'
    },
    targetingTemplates: {
        endpoint: '/rest/targeting_template',
        idField: 'targeting_template_id'
    },
    segmentUploads: {
        endpoint: '/rest/segment_upload',
        idField: 'segment_upload_id'
    },
    segmentCategorySharings: {
        endpoint: '/rest/segment_category_sharing',
        idField: 'segment_category_sharing_id'
    },
    segmentSharings: {
        endpoint: '/rest/segment_sharing',
        idField: 'segment_sharing_id'
    },
    segmentCategoryAssociations: {
        endpoint: '/rest/segment_category_association',
        idField: 'segment_category_association_id'
    },
    segments: {
        endpoint: '/rest/segment',
        idField: 'segment_id'
    },
    segmentCategories: {
        endpoint: '/rest/segment_category',
        idField: 'segment_category_id'
    }
};

function BeeswaxClient(opts) {
    var self = this;

    opts = opts || {};
    if (!opts.creds || !opts.creds.email || !opts.creds.password) {
        throw new Error('Must provide creds object with email + password');
    }
    
    self.apiRoot = opts.apiRoot || 'https://stingersbx.api.beeswax.com';
    self._creds = opts.creds;
    self._cookieJar = rp.jar();
    
    Object.keys(entities).forEach(function(type) {
        var cfg = entities[type];
        self[type] = {};
        self[type].find = self._find.bind(self, cfg.endpoint, cfg.idField);
        self[type].query = self._query.bind(self, cfg.endpoint);
        self[type].queryAll = self._queryAll.bind(self, cfg.endpoint, cfg.idField);
        self[type].create = self._create.bind(self, cfg.endpoint, cfg.idField);
        self[type].edit = self._edit.bind(self, cfg.endpoint, cfg.idField);
        self[type].delete = self._delete.bind(self, cfg.endpoint, cfg.idField);
    });
}

// Send a request to authenticate to Beeswax
BeeswaxClient.prototype.authenticate = function() {
    var self = this;
        
    // Ensure we don't make multiple simulataneous auth requests
    if (self._authPromise) {
        return self._authPromise;
    }
    
    self._authPromise = rp.post({
        url: urlUtils.resolve(self.apiRoot, '/rest/authenticate'),
        body: {
            email: self._creds.email,
            password: self._creds.password,
            keep_logged_in: true // tells Beeswax to use longer lasting sessions
        },
        json: true,
        jar: self._cookieJar
    })
    .then(function(body) {
        if (body.success === false) {
            return Promise.reject(new Error(util.inspect(body)));
        }
    })
    .catch(function(error) {
        delete error.response; // Trim response obj off error for cleanliness
        return Promise.reject(error);
    }).finally(function() {
        delete self._authPromise;
    });
    
    return self._authPromise;
};

// Send a request to Beeswax, handling '401 - Unauthenticated' errors
BeeswaxClient.prototype.request = function(method, opts) {
    var self = this;
    
    opts.json = true;
    opts.jar = self._cookieJar;
    
    return (function sendRequest() {
        return rp[method](opts)
        .catch(rpErrors.StatusCodeError, function(error) {
            if (error.statusCode !== 401) {
                return Promise.reject(error);
            }
            
            return self.authenticate().then(sendRequest);
        });
    }())
    .then(function(body) {
        if (body.success === false) {
            return Promise.reject(new Error(util.inspect(body)));
        }
        return body;
    })
    .catch(function(error) {
        delete error.response; // Trim response obj off error for cleanliness
        return Promise.reject(error);
    });
};

// Send a GET request to find a single entity by id
BeeswaxClient.prototype._find = function(endpoint, idField, id) {
    var opts = {
        url: urlUtils.resolve(this.apiRoot, endpoint),
        body: {}
    };
    opts.body[idField] = id;
    return this.request('get', opts).then(function(body) {
        return { success: true, payload: body.payload[0] };
    });
};

// Send a GET request to fetch entities by JSON query
BeeswaxClient.prototype._query = function(endpoint, body) {
    var opts = {
        url: urlUtils.resolve(this.apiRoot, endpoint),
        body: body || {}
    };
    return this.request('get', opts).then(function(body) {
        return { success: true, payload: body.payload };
    });
};

// Recursively GET entities in batches until all have been fetched.
BeeswaxClient.prototype._queryAll = function(endpoint, idField, body) {
    var self = this,
        results = [],
        batchSize = 50;
    body = body || {};
    
    function fetchBatch(offset) {
        var opts = {
            url: urlUtils.resolve(self.apiRoot, endpoint),
            body: {}
        };
        for (var key in body) {
            opts.body[key] = body[key];
        }
        opts.body.rows = batchSize;
        opts.body.offset = offset;
        opts.body.sort_by = idField;
        
        return self.request('get', opts).then(function(respBody) {
            results = results.concat(respBody.payload);
            
            if (respBody.payload.length < batchSize) {
                return { success: true, payload: results };
            } else {
                return fetchBatch(offset + batchSize);
            }
        });
    }
    
    return fetchBatch(0);
};

// Send a POST request to create a new entity. GETs + resolves with the created entity.
BeeswaxClient.prototype._create = function(endpoint, idField, body) {
    var self = this;
    // Beeswax sends a weird 401 error if a body is empty, so handle this here
    if (!isPOJO(body) || Object.keys(body || {}).length === 0) {
        return Promise.resolve({
            success: false,
            code: 400,
            message: 'Body must be non-empty object',
        });
    }

    var opts = {
        url: urlUtils.resolve(self.apiRoot, endpoint) + '/strict',
        body: body
    };
    return self.request('post', opts).then(function(body) {
        return self._find(endpoint, idField, body.payload.id);
    });
};

// Send a PUT request to edit an existing entity by id. GETs + resolves with the updated entity.
BeeswaxClient.prototype._edit = function(endpoint, idField, id, body, failOnNotFound) {
    var self = this;
    if (!isPOJO(body) || Object.keys(body || {}).length === 0) {
        return Promise.resolve({
            success: false,
            code: 400,
            message: 'Body must be non-empty object',
        });
    }

    var opts = {
        url: urlUtils.resolve(this.apiRoot, endpoint) + '/strict',
        body: body
    };
    opts.body[idField] = id;
    return this.request('put', opts).then(function(/*body*/) {
        return self._find(endpoint, idField, id);
    })
    .catch(function(resp) {
        /* Catch + return "object not found" errors as unsuccessful responses. Can instead set
         * failOnNotFound param to true to reject the original error. */
        var notFound = false;
        try {
            notFound = resp.error.payload[0].message.some(function(str) {
                return (/Could not load object.*to update/).test(str);
            });
        } catch(e) {}
        
        if (!!notFound && !failOnNotFound) {
            return Promise.resolve({
                success: false,
                code: 400,
                message: 'Not found',
            });
        }
        
        return Promise.reject(resp);
    });
};

// Send a DELETE request to delete an entity by id
BeeswaxClient.prototype._delete = function(endpoint, idField, id, failOnNotFound) {
    var opts = {
        url: urlUtils.resolve(this.apiRoot, endpoint) + '/strict',
        body: {}
    };
    opts.body[idField] = id;

    return this.request('del', opts).then(function(body) {
        return { success: true, payload: body.payload[0] };
    })
    .catch(function(resp) {
        /* Catch + return "object not found" errors as unsuccessful responses. Can instead set
         * failOnNotFound param to true to reject the original error. */
        var notFound = false;
        try {
            notFound = resp.error.payload[0].message.some(function(str) {
                return (/Could not load object.*to delete/).test(str);
            });
        } catch(e) {}
        
        if (!!notFound && !failOnNotFound) {
            return Promise.resolve({
                success: false,
                code: 400,
                message: 'Not found',
            });
        }
        
        return Promise.reject(resp);
    });
};

// Send a POST request to upload a segment and attach a file
BeeswaxClient.prototype.uploadSegment = function(params){
    var self = this;

    function init(params){
        var segmentDef = {}, 
            props = ['segment_name', 'segment_type', 'notes', 'active'];

        props.forEach(function(prop){
            if (params[prop]){
                segmentDef[prop] = params[prop];
            }
        });

        if (!params.filePath) {
            return Promise.reject(
                new Error('uploadSegment params requires a filePath property.')
            );
        }

        if (!segmentDef.segment_name){
            segmentDef.segment_name = 
                require('path').basename(params.filePath);
        }

        return Promise.resolve({ req : params, segmentDef : segmentDef });
    }

    function createSegment(data) {
        return new Promise(function(resolve, reject) {
            var formData = {
                segment_name: data.segmentDef.segment_name,
                segment_type: data.segmentDef.segment_type,
                notes: data.segmentDef.notes,
                active: data.segmentDef.active,
                segment_content: {
                    value: require('fs').createReadStream(data.req.filePath),
                    options: {
                        filename: require('path').basename(data.req.filePath),
                        contentType: 'application/octet-stream'
                    }
                }
            };

            var opts = {
                url: urlUtils.resolve(self.apiRoot, '/rest/segment_upload'),
                formData: formData,
                jar: self._cookieJar
            };

            request.post(opts, function(error, response, body) {
                if (error) {
                    return reject(error);
                }
                if (response.statusCode !== 200) {
                    return reject(new Error('Failed to upload segment'));
                }

                resolve(JSON.parse(body));
            });
        });
    }

    function getSegment(data){
        var opts = {
            url: urlUtils.resolve(self.apiRoot, '/rest/segment_upload/' +
                    data.payload.id)
        };

        return self.request('get', opts).then(function(body) {
            return body.payload[0];
        });
    }

    return init(params)
        .then(createSegment)
        .then(getSegment);
};

module.exports = BeeswaxClient;
