# Security Message Bus
Message Bus to managed messages from various security portals in Azure / AWS / GCP and Service Now

The selected languages and components used for this projectt will be leveraging
- python for publisher and subscribers
- python framework ???

Data movement will be supported by the following technologies
- rabbitMQ for the message bus
- MariaDB for storage of messages and message state

This will allow the movement of the messages from various customer environment to starting with Azure.

The infrastructure will be deployed on kerbenettes using Rancher as the Kubernettes manager
