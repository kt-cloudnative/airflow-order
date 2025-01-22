from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='bi-csyscdGrpIdCdRedisJob-batch',WORKFLOW_ID='e1e7bb50db8f4268b6f087e38c998925', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-bi-csyscdGrpIdCdRedisJob-batch-0.0.prd-tz.0.0'
    ,'schedule_interval':'0 0 * * *'
    ,'start_date': datetime(2023, 9, 26, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e1e7bb50db8f4268b6f087e38c998925')

    redisCsyscdGrpIdCdJob_vol = []
    redisCsyscdGrpIdCdJob_volMnt = []
    redisCsyscdGrpIdCdJob_vol.append(getVolume('shared-volume','shared-volume'))
    redisCsyscdGrpIdCdJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    redisCsyscdGrpIdCdJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    redisCsyscdGrpIdCdJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    redisCsyscdGrpIdCdJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    redisCsyscdGrpIdCdJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0d459285e3d04e3ba23cdc72f8199597',
        'volumes': redisCsyscdGrpIdCdJob_vol,
        'volume_mounts': redisCsyscdGrpIdCdJob_volMnt,
        'env_from':redisCsyscdGrpIdCdJob_env,
        'task_id':'redisCsyscdGrpIdCdJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.8',
        'arguments':["--job.name=redisCsyscdGrpIdCdJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e1e7bb50db8f4268b6f087e38c998925')

    workflow = COMMON.getICISPipeline([
        authCheck,
        redisCsyscdGrpIdCdJob,
        Complete
    ]) 

    # authCheck >> redisCsyscdGrpIdCdJob >> Complete
    workflow








