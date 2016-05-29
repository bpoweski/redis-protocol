(ns docker.client
  (:require [taoensso.timbre :as timbre])
  (:import (com.spotify.docker.client DefaultDockerClient DockerClient DockerClient$ListImagesParam DockerClient$ListContainersParam)
           (com.spotify.docker.client.messages ContainerConfig ContainerConfig$Builder)
           (java.nio.file Paths)
           (java.net URI)))


(timbre/refer-timbre)

(defn container-config [m]
  (.build (reduce-kv (fn [builder k v]
                       (case k
                         :image (.image builder v)
                         builder))
                     (ContainerConfig/builder) m)))

(defn ^DockerClient docker-client []
  (.build (DefaultDockerClient/fromEnv)))

(defn docker-pull [^DockerClient client image]
  (.pull client (str image)))

(defn docker-list-images [^DockerClient client]
  (.listImages client (into-array DockerClient$ListImagesParam [(DockerClient$ListImagesParam/allImages)])))

(defn docker-containers [^DockerClient client]
  (.listContainers client (into-array DockerClient$ListContainersParam [])))

(defn docker-create-container [^DockerClient client config]
  (.createContainer client (container-config config)))

(defn repo-tags [img]
  (into #{} (.repoTags img)))

(defn tag? [img tag]
  (contains? (repo-tags img) tag))

(defn find-or-pull-image [^DockerClient client image-tag]
  (if-let [[img & _] (filter #(tag? % image-tag) (docker-list-images client))]
    img
    (do (debug "pulling docker image:" image-tag)
        (docker-pull client image-tag)
        (first (filter #(tag? % image-tag) (docker-list-images client))))))
