var gulp = require("gulp");
var browserify = require('gulp-browserify');

gulp.task("default", function () {
  return gulp.src("./src/script.js")
  .pipe(browserify())
  .pipe(gulp.dest("./"));
});
