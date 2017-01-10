const gulp = require("gulp");
const browserify = require('gulp-browserify');

const destination = "../src/main/resources/web"

gulp.task("scripts", () => {
  return gulp.src("./src/script.js")
  .pipe(browserify())
  .pipe(gulp.dest(destination));
});

gulp.task("move", () => {
  return gulp.src(["./index.html", "./styles.css", "./images/**/*.*"], { base: "./" })
  .pipe(gulp.dest(destination))
});

gulp.task("default", ["scripts", "move"])
