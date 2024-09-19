# Spark at the ONS

Spark learning materials compiled by members of the Office for National Statistics. 
<!--Should there be more detail here about what kinds of content we have in the book? -->

## Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given. You can contribute in the ways listed below.

We have recently made use of GitHub issue forms which we hope will make suggesting content even easier!
you want to suggest a new page, please use [this link for the issue form](https://github.com/best-practice-and-impact/ons-spark/issues/new?assignees=&labels=New+page&projects=&template=new-page-form.yml&title=%5BNew+page%5D%3A+)
For bug issues please use [this link for the issue form](https://github.com/best-practice-and-impact/ons-spark/issues/new?assignees=&labels=bug&projects=&template=bug-report-form.yml&title=%5BBug%5D%3A+)
To create any other type of issue, use the blank issue form and label your issue with any of the following tags:

| label    | description |
| - | - |
| Bug | Used for general bug reports    |
| Documentation | Improvements or additions to documentation |
| Duplicate | This issue or pull request already exists |
| Help wanted | External help or guidance (outside of DAPCATS) is needed before this can be implemented |
| Question | Further information is needed or something needs clarification |
| New page | Suggestions for new topics and pages |
| Addition to existing page | Suggestions to add information to existing pages e.g. Python/R equivalent code, new subsection etc. |
| Enhancement | Suggestion for change not relating to content eg. improving the usability or appearance of the book |
| Other | An issue that is not covered by any of the other labels |

If you wish to take an active role in developing this book please refer to the [contributing guidance](./CONTRIBUTING.md)

<!-- Does this section belong in the Readme? Or should we point towards the contributing guide as above? -->
### Building the book

If you'd like to develop and/or build the Spark at the ONS book, you should:

1. Clone this repository
2. Run `pip install -r requirements.txt` (it is recommended you do this within a virtual environment)
3. (Optional) Edit the books source files located in the `ons-spark/` directory
4. Run `jupyter-book clean ons-spark/` to remove any existing builds
5. Run `jupyter-book build ons-spark/`
6. Check the built HTML pages are correct (these will be in ons-spark/_build/html)
7. Once you are happy, to deploy the book online run `ghp-import -n -p -f ons-spark/_build/html`.
8. Note you may need to do `pip install ghp-import` if you do not already have this installed.

A fully-rendered HTML version of the book will be built in `ons-spark/_build/html/`.

### Hosting the book

Please see the [Jupyter Book documentation](https://jupyterbook.org/publish/web.html) to discover options for deploying a book online using services such as GitHub, GitLab, or Netlify.

For GitHub and GitLab deployment specifically, the [cookiecutter-jupyter-book](https://github.com/executablebooks/cookiecutter-jupyter-book) includes templates for, and information about, optional continuous integration (CI) workflow files to help easily and automatically deploy books online with GitHub or GitLab. For example, if you chose `github` for the `include_ci` cookiecutter option, your book template was created with a GitHub actions workflow file that, once pushed to GitHub, automatically renders and pushes your book to the `gh-pages` branch of your repo and hosts it on GitHub Pages when a push or pull request is made to the main branch.

## Contributors

We welcome and recognize all contributions. You can see a list of current contributors in the [contributors tab](https://github.com/best-practice-and-impact/ons-spark/graphs/contributors).

## Credits

This project is created using the excellent open source [Jupyter Book project](https://jupyterbook.org/) and the [executablebooks/cookiecutter-jupyter-book template](https://github.com/executablebooks/cookiecutter-jupyter-book).
 
