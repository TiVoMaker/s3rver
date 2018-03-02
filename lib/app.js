"use strict";

const express = require("express");
const morgan = require("morgan");
const path = require("path");

const controllers = require("./controllers");
const cors = require("./cors");
const createLogger = require("./logger");
const S3Logger = require('./s3logging');
const Subject = require("rxjs/Subject").Subject;
require("rxjs/add/operator/filter");

module.exports = function(options) {
  const app = express();
  const doLogging = options.logBucket && options.bucketsToLog.length > 0;
  app.s3Event = new Subject();

  /**
   * Log all requests
   */
  app.use(
    morgan("tiny", {
      stream: {
        write(message) {
          app.logger.info(message.slice(0, -1));
        }
      }
    })
  );

  if (doLogging) {
    app.use(
      morgan((tokens, req, res) => {
          app.S3Logger.logEntry({
              url: tokens.url(req, res),
              method: tokens.method(req, res),
              date: tokens.date(req, res, 'clf'),
              remoteAddr: tokens['remote-addr'](req, res),
              httpVersion: tokens['http-version'](req, res),
              status: tokens.status(req, res),
              length: tokens.res(req, res, 'content-length'),
              referrer: tokens.referrer(req, res),
              userAgent: tokens['user-agent'](req, res),
              headers: req.headers
          });
          return "";
      }, {
        stream: {
          write() {}
        }
      })
    );
  }

  app.use((req, res, next) => {
    const host = req.headers.host.split(":")[0];

    // Handle requests for <bucket>.s3(-<region>?).amazonaws.com, if they arrive.
    const bucket = (/(.+)\.s3(-.+)?\.amazonaws\.com$/.exec(host) || [])[1];
    if (bucket) {
      req.url = path.join("/", bucket, req.url);
    } else if (
      options.indexDocument &&
      host !== "localhost" &&
      host !== "127.0.0.1"
    ) {
      req.url = path.join("/", host, req.url);
    }

    next();
  });

  app.use(cors(options.cors));

  app.disable("x-powered-by");

  // Don't register logger until app is successfully set up
  app.logger = createLogger(options.silent);

  const middleware = controllers(
    options.directory,
    app.logger,
    options.indexDocument,
    options.errorDocument
  );

  // Get the S3 logger going, if desired.
  if (doLogging) {
      app.S3Logger = new S3Logger(
          options.directory,
          app.logger,
          options.logBucket,
          options.logPrefix,
          options.logMaxDelay,
          options.bucketsToLog
      );
  }

  /**
   * Routes for the application
   */
  app.get("/", middleware.getBuckets);
  app.get("/:bucket", middleware.bucketExists, middleware.getBucket);
  app.delete("/:bucket", middleware.bucketExists, middleware.deleteBucket);
  app.put("/:bucket", middleware.putBucket);
  app.put("/:bucket/:key(*)", middleware.bucketExists, middleware.putObject);
  app.post("/:bucket/:key(*)", middleware.bucketExists, middleware.postObject);
  app.get("/:bucket/:key(*)", middleware.bucketExists, middleware.getObject);
  app.head("/:bucket/:key(*)", middleware.getObject);
  app.delete(
    "/:bucket/:key(*)",
    middleware.bucketExists,
    middleware.deleteObject
  );
  app.post("/:bucket", middleware.bucketExists, middleware.genericPost);

  return app;
};
