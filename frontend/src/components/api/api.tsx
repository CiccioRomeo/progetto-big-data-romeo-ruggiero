import axios from "axios";
import type { TagsResponse, AssociationRule, CoordinateData, KMeansResponse, PhotoSearched, PhotoCount, HourData, OwnerSearched, PopularOwner, FirstPost, AvgData, ProVsNonProResponse, TopCameraPerYear, SearchedCamera, TopBrandAndModels, TotalUsers, AvgMdnViews, AvgMdnComments, AvgViewsYear, TotalRows, AvgCommentsYear, Accuracy, TotalCameraBrands } from "@/lib/types";

const api = axios.create({
  baseURL: "https://bqgq98pb-8080.euw.devtunnels.ms", 
  //baseURL: "https://fp9w3qjx-8080.euw.devtunnels.ms", 
  //baseURL: "http://127.0.0.1:8081", 
});


// ########## MAPS ##########

// Home
export const getTotalRows = async (): Promise<TotalRows> => {
  const response = await api.get(`/countRows`);
  return response.data;
};

export const getTotalUsers = async (): Promise<TotalUsers> => {
  const response = await api.get(`/countUsers`);
  return response.data;
};

export const getTotalCameras = async (): Promise<TotalCameraBrands> => {
  const response = await api.get(`/brandCount`);
  return response.data;
};

export const getViewsStats = async (): Promise<AvgMdnViews[]> => {
  const response = await api.get(`/viewStats`);
  return response.data;
};

export const getCommentsStats = async (): Promise<AvgMdnComments[]> => {
  const response = await api.get(`/commentStats`);
  return response.data;
};

export const getAvgViewsPerYear = async (): Promise<AvgViewsYear[]> => {
  const response = await api.get(`/avgViewsPerYear`);
  return response.data;
};

export const getAvgCommentsPerYear = async (): Promise<AvgCommentsYear[]> => {
  const response = await api.get(`/avgCommentsPerYear`);
  return response.data;
};

export const getAccuracyDistribution = async (): Promise<Accuracy[]> => {
  const response = await api.get(`/accuracyDistribution`);
  return response.data;
};

export const getCoordinates = async (): Promise<CoordinateData[]> => {
  const response = await api.get(`/photosByCoordinates`);
  return response.data;
};

// Clusters
export const KMeans = async (k: number): Promise<KMeansResponse> => {
  const response = await api.get(`/runKMeans`, {params: { k }});
  return response.data;
};

// ########## ANALYTICS ##########

// CameraAnalytics
export const getTopCamerasPerYear = async (): Promise<TopCameraPerYear[]> =>{
  const response = await api.get(`/topCamerasPerYear`);
  return response.data;
}

export const getTopBrandsAndModels = async (): Promise<TopBrandAndModels[]> =>{
  const response = await api.get(`/getTopBrandAndCameras`);
  return response.data;
}

export const getCamerasInfo = async (brand: string): Promise<SearchedCamera[]> => {
  const response = await api.get(`/searchBrandsInfo`, { params: { brand} });
  return response.data;
};

// PhotoTrends
export const getPhotoTakenCount = async (): Promise<PhotoCount> => {
  const response = await api.get(`/photoCountTaken`);
  return response.data;
};

export const getPhotoPostedCount = async (): Promise<PhotoCount> => {
  const response = await api.get(`/photoCountPosted`);
  return response.data;
};

export const getPhotoPerMonthByYear = async (year: number, type: string): Promise<any[]> => {
  const response = await api.get(`/photoPostedPerMonthByYear`,{ params: { year, type } });
  return response.data;
};

export const getPhotoCountHour = async (): Promise<HourData> => {
  const response = await api.get(`/photoCountHour`);
  return response.data;
};

export const getAvgTimeToPost = async (): Promise<AvgData[]> => {
  const response = await api.get(`/averageTimeToPost`)
  return response.data;
}

// Tag Analytitcs
export const topTags = async (limit?: number): Promise<TagsResponse[]> => {
  const response = await api.get(`/topTags`, { params: { limit } });
  return response.data;
};

export const getAssociationRules = async(min_support: number, min_confidence: number, target_tags: string[] = []): Promise<AssociationRule[]> => {
  try {
    const response = await api.post(`/tagAssociationRules`, {min_support, min_confidence, target_tags});
    return response.data;
  } catch (error) {
    console.error("Error searching rules:", error);
    throw error;
  }
};

// Users Analytitcs
export const getOwners = async (username: string): Promise<OwnerSearched[]> => {
  const response = await api.get(`/searchOwnerCam`, { params: { username} });
  return response.data;
};

export const getPopularOwners = async (): Promise<PopularOwner[]> => {
  const response = await api.get(`/top50Owners`);
  return response.data;
};

export const getFirstPostPerYear = async (): Promise<FirstPost[]> => {
  const response = await api.get(`/firstPostPerYear`);
  return response.data;
};

export const getProVsNonPro = async (): Promise<ProVsNonProResponse[]> =>{
  const response = await api.get(`/proUsersDistribution`);
  return response.data;
}

// ########## PHOTO-SEARCH ##########

// PhotoSearch
export const getPhotos = async (keyword: string, dataInizio: string, dataFine: string, tagList: string[] = []): Promise<PhotoSearched[]> => {
  try {
    const response = await api.post(`/searchPhotos`, {keyword, dataInizio, dataFine, tag_list: tagList});
    return response.data;
  } catch (error) {
    console.error("Error searching photos:", error);
    throw error;
  }
};

