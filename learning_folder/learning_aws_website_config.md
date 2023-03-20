# Guideline to configure websties on AWS


Tech stack: Node + express + mySQL, 
## Procedure
1. Launch a virtual machine on AWS
    1. Select t3.small (2GiB memory) as instance type with the motivation that we need to run dockerfile on the server
    2. Request Spot Instance (with the price of 0.0068$)
    3. Add HTTP Type with port number of 80 # (Use AWS certificate manager to get https)
                                            # (The idea is that the internal request is gone though http, the external request is applied with `Load Balancer` to gain https.)
                                            # (`Load balancer` can access the service of AWS certificate management. The external visit is shown with https.)
    4. upload file to ec2 virtual machine
        `scp -i /path/to/key.pem /path/to/sourcefile ubuntu@ec2-IP.eu-north-1.compute.amazonaws.com:~/`

        check `Public IPv4 DNS` on amazon console 
    5. Go to `/node-docker-test/docker-deploy/`
    *  (Optional: install docker `https://docs.docker.com/engine/install/ubuntu/`)
    *  (bash script of installing dockr is in ./trash/docker.sh, copy it into vitual machine and ./docker.sh, it will auto install the docker and docker-compose)
    *  (Optional: `sudo chmod 666 /var/run/docker.sock` to fix the bug of 
        `Got permission denied while trying to connect to the Docker daemon sockat at unix...`)
        (Optional: install docker-compose `https://docs.docker.com/engine/install/ubuntu/`)
        (Solution to the problem of `ERROR: failed to update bridge store for object type *bridge.networkConfiguration: open /var/lib/docker/network/files/local-kv.db: no such file or directory`
        * remove the Docker internal files
        `sudo rm -rf /var/lib/docker`
        * restart the Docker Daemon service
        `sudo systemctl restart docker`)
    6. `sudo docker-compose up -d`
    7. Use browser to check if the VM is correctly set up: `http://IP-ADDRESS/hello`
    8. Bundle with our customized URL address
        * There are several methods to achieve URL customization. (`Load Balancer` is the one used in our case)
        1. create target group (even though we have only one virtual machine at hands). The services included in dockerfile are forwarded into this target group through `Load Balancer`.
        2. Name the target group; select 80 as the port number for visit; use `/hello` as input to Health check path
        3. Select available instances into target group and click `Create target group`
        4. Create `Application Load Balancer`
        5. Select all regions to avoid being crashed when the current used machine room is not available
        6. Besides the default one, add the one with https into security group (the exposed port is 443)
        7. Under `Listeners and routing`, select HTTPS as protocol, 443 as Port, the created target group (express1 in our case) as the location being forwarded to.
        8. `quokkaai.se` is alreadly registered on aws and its certificate is issued. We select `quokkaai.se` from ACM
        9. Create `Load Balancer` (It can take a while)
        10. Go to `Route 53` service on AWS --> Hosted zones where you can define customized subdomains.
        11. To create a subdomaim (`create record`), we point our Route traffic to `Alias to Application and Classic Load Balancer` in `Europe(Stockholm)[eu-north-1]` with `CREATED LOAD BALANCER`
        12. Congrats!

    9. The process how we destroy the resources
        1. Go to the target group, deregister instances.
        2. Go to Load Balancer, remove listener
        3. Delete target group

2. Run docker file (containing our script, data, database, restAPI in express.js, react.js )

3. How to update server.js file
   
   - go to the folder containing server.js
   - change everthing you want to change
   - save it and rebuild the image `sudo docker build . -f Dockerfile --tag jiajidev/express-server-sample:0.2`
     building currently folder to the image using the Dockerfile. `jiajidev/express-server-sample:0.2` is the name of the image.
   - back to the `/node-docker-test/docker-deploy/` run `sudo docker-compose up -d` update the image
   - refresh the website to get the new version

# Questions:
1. How to mirror virtual machine?

# Tips:
* use docker-compose to combine multiple dockers in one single network
* How to update our scripts or data (one brutal approach: delete docker-compose.yml and create a new)
* Use postman to test different HTTP methos with endpoints.

1. Go to `node-docker/node-docker-test/docker-deploy`
2. execute `sudo docker-compose up`
3. PORT=8080
4. possible to modify the port number in .envs