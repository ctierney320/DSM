# July 19, 2021
# Charlotte Tierney


###### Pseudo code
# For each set of covariates, input the memory options for your machine, 
# the location you want the tiles saved, the location of your covariates,  
# (if needed) the location of a shapefile of your study area extent,
# and designate the size of each tile

#for first raster:
  #read in
  #make df with data, ID#, x, and y
  #tile into N tiles based on number of obs

#for each following raster in list:
  #read in
  #for each existing tile
  #read in the tile
  #subset covariate df to match tile
  #match new covariate and tile, save




###### Workplace Setup
## Packages etc:
required.packages <- c("plyr","raster", "sp", "rgdal", "snow", 
                       "snowfall", "randomForestSRC","beepr",
                       "dplyr", "ggplot2","hexbin","permute") #randomForest #quantregForest
new.packages <- required.packages[!(required.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(required.packages, require, character.only=T)
rm(required.packages, new.packages)

#### Set memory options for your machine:
memory.limit(500000)
rasterOptions(maxmemory = 1e+09, chunksize = 1e+08)


## Location for output files:
localfolder <- "C:/Your/Local/Work/Folder/" #make sure string includes the last "/"

## Name of tiling folder:
tilefold <- "CovTiles"

## Location of rasters:
covfolder <- "C:/Your/CovFolder/Path/" #make sure string includes the last "/"

cov.grids <- list.files(path = covfolder, pattern=".tif$", 
                        full.names = T, recursive = F)
#edit these based on folder:
cov.names <- gsub(".tif","", cov.grids)# remove .tif to isolate covariate names
cov.names <- gsub("C:/Your/CovFolder/Path/",
                  "", cov.names)   #remove file path to isolate covariate names
covs.projection <- projection(raster(cov.grids[1]))

# ## Shapefile for study extent: #if needed
# extent <- readOGR("C:/Your/Shapefile/Location.shp")

## Decide on number of pixels per tile (test this for your machine)
batchsize = 1000000                                                             #1 million works well for RFSRC on WSCR062


###### Workflow

#### First Raster
r <- raster(cov.grids[1])
#r <- crop(r,extent)
start.time <- Sys.time()
xyz <- raster::as.data.frame(r, xy=TRUE)
xyz$ID <- 1:nrow(xyz)
xyz$x <- as.integer(xyz$x)
xyz$y <- as.integer(xyz$y);beep(sound=1)
end.time <- Sys.time()
FirstXYZ.time <- end.time - start.time
FirstXYZ.time

#### Tiling the First Raster
## Create new folder for tiles using tilefold name created above
dir.create(path=paste(localfolder,tilefold,sep=""))                             #ONLY NEEDED ONCE

#make one string for the tile folder
tilefolder <- paste(localfolder,tilefold,"/",sep="")


## Specs:
batchsize = batchsize # no. of obs per tile 

## Creating all tiles (based on first covariate)
start.time <- Sys.time()
for (i in 1:ceiling(nrow(xyz)/batchsize)){                         
  batch <- xyz[((((i-1)*batchsize)+1):(batchsize*i)),]#,,drop=FALSE] #drop=FALSE keeps it from being converted to a vector
  batch <- batch[!is.na(batch$ID),] # gets rid of remainder NULLs (NOT rows with NA values)
  saveRDS(batch, paste(tilefolder,"cov_tile",i,".rds",sep=""))
}
end.time <- Sys.time()
FirstTile.time <- end.time - start.time
FirstTile.time

## Reference the tiles:
tiles <- list.files(path = tilefolder,
                    pattern=".rds$", full.names = T, recursive = F)





#### Adding the rest of the covariates to existing tiles:
## if you haven't already:
tilefolder <- paste(localfolder,tilefold,"/",sep="")
tiles <- list.files(path = tilefolder,
                    pattern=".rds$", full.names = T, recursive = F)
## Specs:
len <- length(cov.grids)
ntiles <- length(tiles)

tile.fn <- function(x){
  r <- raster(cov.grids[x]); #read in next raster
  #r <- crop(r,extent)
  xyz.temp <- raster::as.data.frame(r, xy=TRUE) #convert it
  xyz.temp$ID <- 1:nrow(xyz.temp)
  xyz.temp$x <- as.integer(xyz.temp$x)
  xyz.temp$y <- as.integer(xyz.temp$y);
  for (t in 1:ntiles){                                                          #LIMIT WHEN TESTING #should be (t in 1:ntiles)
    tile <- readRDS(paste(tilefolder,"cov_tile",t,".rds",sep="")) #read in one tile at a time
    tile <- tile[order(tile$ID),]
    tile.min <- as.numeric(tile$ID[1])
    tile.max <- as.numeric(tail(tile$ID,1))
    batch <- xyz.temp[tile.min:tile.max,] #subset based on tile ID min and max
    batch <- merge(tile, batch, by = c("ID","x","y"))                           #CHECK THIS STEP DURING TESTING
    batch <- batch[order(batch$ID),]
    saveRDS(batch, paste(tilefolder,"cov_tile",t,".rds",sep=""))
    rm(tile,batch,tile.min,tile.max)
  }
  rm(xyz.temp,r)
  return(cov.names[x])
}

# tile.fn(3) #for testing where () is covariate

start.time <- Sys.time()
cpus <- parallel::detectCores() - 2
snowfall::sfInit(parallel=TRUE, cpus=cpus)
snowfall::sfExport("tile.fn","tiles","len","ntiles",
                   "cov.names","cov.grids","tilefolder") 
snowfall::sfLibrary(raster)
tiling_check <- snowfall::sfLapply(2:len, function(x){tile.fn(x)})              #LIMIT WHEN TESTING #should be 2:len
snowfall::sfStop()

end.time <- Sys.time()
AllTiles.time <- end.time - start.time
AllTiles.time


## Read in a file to check
tile_check <- readRDS(paste(tilefolder,"cov_tile13.rds",sep=""))
summary(tile_check)




# Done 

