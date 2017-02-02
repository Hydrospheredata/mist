const gulp = require("gulp");
const browserify = require('gulp-browserify');
const concat = require('gulp-concat');
const cssmin = require('gulp-cssmin');
const rename = require('gulp-rename');
const minify = require('gulp-minify');
const htmlmin = require('gulp-htmlmin');

const destination = "../src/main/resources/web"

gulp.task("scripts", () => {
  return gulp.src("./src/script.js")
  .pipe(browserify())
  .pipe(minify({noSource: true}))
  .pipe(gulp.dest(destination));
});

gulp.task("images", () => {
  return gulp.src("./images/**/*.*", { base: "./" })
  .pipe(gulp.dest(destination))
});

gulp.task("styles", () => {
  return gulp.src(["./styles.css", "./node_modules/codemirror/lib/codemirror.css"], { base: "./" })
    .pipe(concat("all.css"))
    .pipe(cssmin())
    .pipe(rename({ suffix: '.min' }))
    .pipe(gulp.dest(destination));
});

gulp.task("html", () => {
  return gulp.src("./index.html")
    .pipe(htmlmin({collapseWhitespace: true, removeComments: true, removeOptionalTags: true}))
    .pipe(gulp.dest(destination));
});

gulp.task("default", ["scripts", "styles", "html", "images"])
