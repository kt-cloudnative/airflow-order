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
                , WORKFLOW_NAME='EI_CSAV106B',WORKFLOW_ID='81347b9d7c2846bebec65035d179d1b8', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CSAV106B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('81347b9d7c2846bebec65035d179d1b8')

    csav106bJob_vol = []
    csav106bJob_volMnt = []
    csav106bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csav106bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csav106bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csav106bJob_env.extend([getICISConfigMap('icis-oder-entprinet-batch-mng-configmap'), getICISSecret('icis-oder-entprinet-batch-mng-secret'), getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISSecret('icis-oder-entprinet-batch-secret')])
    csav106bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csav106bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '92894fc242d24a0280b52df826b1ee0d',
        'volumes': csav106bJob_vol,
        'volume_mounts': csav106bJob_volMnt,
        'env_from':csav106bJob_env,
        'task_id':'csav106bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.33',
        'arguments':["--job.name=csav106bJob", "requestDate=${YYYYMMDDHHMISS}", "procDate=20241129", "progName=csav106b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    inareainetAutoTrmnTrtJob_vol = []
    inareainetAutoTrmnTrtJob_volMnt = []
    inareainetAutoTrmnTrtJob_vol.append(getVolume('shared-volume','shared-volume'))
    inareainetAutoTrmnTrtJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    inareainetAutoTrmnTrtJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    inareainetAutoTrmnTrtJob_env.extend([getICISConfigMap('icis-oder-entprinet-batch-mng-configmap'), getICISSecret('icis-oder-entprinet-batch-mng-secret'), getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISSecret('icis-oder-entprinet-batch-secret')])
    inareainetAutoTrmnTrtJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    inareainetAutoTrmnTrtJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5614d3b58d284f0cad405a15572df458',
        'volumes': inareainetAutoTrmnTrtJob_vol,
        'volume_mounts': inareainetAutoTrmnTrtJob_volMnt,
        'env_from':inareainetAutoTrmnTrtJob_env,
        'task_id':'inareainetAutoTrmnTrtJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.33',
        'arguments':["--job.name=inareainetAutoTrmnTrtJob", "requestDate=${YYYYMMDDHHMISS}", "procDate=20241129", "progName=pidodei0747"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('81347b9d7c2846bebec65035d179d1b8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csav106bJob,
        inareainetAutoTrmnTrtJob,
        Complete
    ]) 

    # authCheck >> csav106bJob >> inareainetAutoTrmnTrtJob >> Complete
    workflow








