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
                , WORKFLOW_NAME='bi-csyscdGrpIdRedisJob2-batch',WORKFLOW_ID='e8c74deb24e24b708cea3cab844584ad', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-bi-csyscdGrpIdRedisJob2-batch-0.4.prd-tz.0.0'
    ,'schedule_interval':'0 0 * * *'
    ,'start_date': datetime(2024, 6, 1, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e8c74deb24e24b708cea3cab844584ad')

    redisCsyscdGrpIdJob_vol = []
    redisCsyscdGrpIdJob_volMnt = []
    redisCsyscdGrpIdJob_vol.append(getVolume('shared-volume','shared-volume'))
    redisCsyscdGrpIdJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    redisCsyscdGrpIdJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    redisCsyscdGrpIdJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    redisCsyscdGrpIdJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    redisCsyscdGrpIdJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0df34c2dc43e4fdd90e2f5355eb9168b',
        'volumes': redisCsyscdGrpIdJob_vol,
        'volume_mounts': redisCsyscdGrpIdJob_volMnt,
        'env_from':redisCsyscdGrpIdJob_env,
        'task_id':'redisCsyscdGrpIdJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.8',
        'arguments':["--job.name=redisCsyscdGrpIdJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e8c74deb24e24b708cea3cab844584ad')

    workflow = COMMON.getICISPipeline([
        authCheck,
        redisCsyscdGrpIdJob,
        Complete
    ]) 

    # authCheck >> redisCsyscdGrpIdJob >> Complete
    workflow








