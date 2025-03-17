# Detection

This project is a major part of detection processing because when you scale up the processing unit, it needs this image to run processing algorithms. You need to build this image and push it to your preferred docker registry. (I prefer to use `Azure Container Registry` because it's well integrated with Azure services)

### How to build image

1. Clone the repository
2. Run the following command to build the image:

```bash
docker build -t <your_image_name> .
```

3. Push the image to your preferred container registry:

```bash
docker tag <your_image_name> <your_registry>/<your_image_name>:<tag>
docker push <your_registry>/<your_image_name>:<tag>
```

## Where to use this image

This image will be used in `Azure Logic App` configuration workflows as the base image that will run when the workflow is triggered.
