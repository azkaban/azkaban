# Azkaban GitHub pages

View the page at [Azkaban Github Page](http://azkaban.github.io/azkaban).

## Set up
One can follow the guide [here](https://help.github.com/articles/using-jekyll-with-pages/) or follow the steps below.

### Install bundler
Run this from shell
```
gem install bundler
```
This will use the system default version of `gem` and you will need sudo permissions for this. A possible alternative is to install using `rvm` which creates sort of a virtual environment for `ruby`.
1. Follow instructions to install `rvm` from [rvm.io](http://rvm.io).
2. Reload bash profile using `source ~/.profile`.
3. Run `rvm list known` to get the list of installable Ruby interpreters.
4. Mac users might need to install `xcode`. You can do so using `sudo xcode-select --install`.
5. Install your preferred version of Ruby from the list. Example: `rvm install ruby-2.3.3`
6. Check your Ruby version using `ruby --version`.
7. You are all set! Now your `ruby` and `gem` should point to your local versions instead of system versions. Run the `gem install` command mentioned above.

### Start the gh-pages server
This reads GemFile and installs all dependencies
```
bundle install
```
Start the server. The port config is available in `_config.yml`. Alternatively pass `--port 4444` arguments to the following command to customize port.
```
bundle exec jekyll serve
```
