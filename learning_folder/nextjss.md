# Install nextjs

test test test

1. set up the node environment
   
   type `node --version` check whether you already install node.

   Due to the ubuntu version of nodejs, we can not directly use npm create or yarn create, so we should upgrade nodejs version

   Get the current version: 
   Node.js Current:

    `curl -fsSL https://deb.nodesource.com/setup_current.x | sudo -E bash - sudo apt-get install -y nodejs`

    try `node --version` again, make sure the version is over 14.x

2. install yarn(if you prefer to use npm, go to step 3 directly)
   
   run `corepack enable` if your node version >= 16.10

   type `which yarn` to see where yarn is located

   if it is in `usr/bin/yarn`, it works,
   
   otherwise move yarn from result `path/to/file` to `/usr/bin/yarn`
   as well as yarnpkg`

3. start a new yarn next.js project
   
   `yarn create next-app --example blog-starter blog-starter-app`

   `npx create-next-app --example blog-starter blog-starter-app`

4. Everything done.
