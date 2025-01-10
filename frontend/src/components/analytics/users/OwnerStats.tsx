import styles from './owner-card.module.css'
import { formatNumber } from "@/components/maps/clusters/utils";
import { OwnerSearched } from "@/lib/types";
import { Camera, Eye, MessageCircleMore } from "lucide-react";

interface OwnerStatsProps{
    owner: OwnerSearched;
}

export function OwnerStats({ owner }: OwnerStatsProps){
    return (
        <div className={styles.stats}>
            <div className={styles.name}>{owner.username}</div>
            <div className={styles.statItem}>
                <Camera className={styles.icon} />
                <span>{formatNumber(owner.total_photos)} foto</span>
            </div>
            <div className={styles.statItem}>
                <Eye className={styles.icon} />
                <span>{formatNumber(owner.total_views)} visualizzazioni</span>
            </div>
            <div className={styles.statItem}>
                <MessageCircleMore className={styles.icon} />
                <span>{formatNumber(owner.total_comments)} Commenti</span>
            </div>
        </div>
    )
}
