
## Build commands

`npx likec4 codegen react --no-use-dot -o ../likec4_react/likec4.generated.js`

Note that `--no-use-dot` is needed for general containers which aren't the likec4 dedicated container.


## Index image generation

This uses a headless browser session, so the dependencies for playwright have to be installed first
`npx playwright install`
`npx playwright install-deps`

Then run the export command
`npx likec4 export png --no-use-dot`
Copy the index figure into the public images directory
`cp index.png ../public/images/likec4_index.png`
