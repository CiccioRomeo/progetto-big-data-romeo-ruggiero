// Home

export type TotalRows = number;

export type TotalUsers = number;

export type TotalCameraBrands = number;

export interface  AvgMdnViews{
  average_views: number;
  median_views: number;
}

export interface  AvgMdnComments{
  average_comments: number;
  median_comments: number;
}

export interface  AvgViewsYear{
  average_views: number;
  yearPosted: number;
}

export interface  AvgCommentsYear{
  average_comments: number;
  yearPosted: number;
}

export interface  Accuracy{
  accuracy: number;
  count: number;
}

export interface StatsOverviewProps {
  totalRows: TotalRows
  totalUsers: TotalUsers;
  totalBrands: TotalCameraBrands;
  viewsStats: AvgMdnViews
  commentsStats: AvgMdnComments;
  yearlyViews: AvgViewsYear[];
  yearlyComments: AvgCommentsYear[];
  accuracyDistribution: Accuracy[];
}

export type CoordinateData = [number, number, number]; // [latitude, longitude, intensity]
export interface Coordinate {
  position: [number, number];
  intensity: number;
}

//######################################

// Cluster
export type ClusterData = [number, number, number]; // [latitude, longitude, intensity]
export type Cluster = {
  latitude: number;
  longitude: number;
  label: number;
};

export type CentroidsData = [number, number];
export type Centroids = {
  latitude: number;
  longitude: number;
};


export type CentroidDistance = {
  label: number;
  latitude: number;
  longitude: number;
};
export type DistancesData = {
  distanza: number;
  latitudeM: number;
  longitudeM: number;
  monumento: string;
};
export type Distances = {
  centroid: CentroidDistance;
  distances: DistancesData[];
};

export interface KMeansResponse {
  labels: ClusterData[]; 
  centroids: CentroidsData[]; 
  distanze: Distances[];
}

//######################################

// Trend fotografici

export type MonthData = [string, number];
export type YearData = [number, number];
export interface PhotoCount {
  month_data: MonthData[];
  year_data: YearData[];
}

export type HourCountData = [number, number];
export interface HourData {
  posted: HourCountData[];
  taken: HourCountData[];
}

//######################################

// Analisi delle camere

export interface TopCameraPerYear{
  count: number;
  make: string;
  model: string;
  year: number;
}

export interface CameraSearch {
  brand: string;
}

export type SearchedCamera = [string, number];

export type Models = [string, number];
export interface TopBrandAndModels{
  brand: string;
  models_list: Models[];
  rank: number;
  total_count: number;
}

//######################################

// Analisi dei tag

export interface TagsResponse {
  count: number;
  tagValue: string;
}

export interface AssociationRule {
  antecedent: string[];
  confidence: number;
  consequent: string[];
  lift: number;
  support: number;
}

export interface RuleMetrics {
  confidence: number;
  lift: number;
  support: number;
}

export interface RuleSearchFilters {
  minSupport: number;
  minConfidence: number;
  tags: string[];
}

export interface AvgData{
  averageTimeToPostMinutes: number;
} 

//######################################

// Analisi degli utenti

export interface Photo {
  url: string;
  views: number;
  comments: number;
}

export type CameraInfo = [string, string, number];
export interface OwnerSearched {
  avatar_url: string;
  best_photo_url: string;
  camera_details: CameraInfo[];
  most_viewed_photo_comments: number;
  most_viewed_photo_views: number;
  rank: number;
  total_comments: number;
  total_photos: number;
  total_views: number;
  owner_id: string;
  username: string;
}

export interface PopularOwner {
  total_views: number;
  username: string;
}

export interface UserSearchFilters {
  owner: string;
}

export interface MonthDataCount{
  count: number;
  month: number;
}

export interface FirstPost{
  year: number;
  months: MonthDataCount[]
};

export interface ProVsNonProResponse{
  pro: boolean;
  user_count: number;
}

//######################################

// Ricerca foto

export interface PhotoSearched {
  datePosted: string;
  dateTaken: string;
  tags: string[];
  title: string;
  url: string;
  username: string;
  views: number;
}

export interface PhotoSearchFilters {
  startDate: string;
  endDate: string;
  keyword: string;
  tags: string[];
}
