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
                , WORKFLOW_NAME='IC_CNTRYINFOAUTOTRMNTRT',WORKFLOW_ID='db88c6972f394ed199b9c81be9fa180e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CNTRYINFOAUTOTRMNTRT-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 6, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('db88c6972f394ed199b9c81be9fa180e')

    cntryInfoAutoTrmnTrtJob_vol = []
    cntryInfoAutoTrmnTrtJob_volMnt = []
    cntryInfoAutoTrmnTrtJob_vol.append(getVolume('shared-volume','shared-volume'))
    cntryInfoAutoTrmnTrtJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cntryInfoAutoTrmnTrtJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cntryInfoAutoTrmnTrtJob_env.extend([getICISConfigMap('icis-oder-infocomm-batch-mng-configmap'), getICISSecret('icis-oder-infocomm-batch-mng-secret'), getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISSecret('icis-oder-infocomm-batch-secret')])
    cntryInfoAutoTrmnTrtJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cntryInfoAutoTrmnTrtJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ea84ac9b2d3b41fd80c040d8ae3261e5',
        'volumes': cntryInfoAutoTrmnTrtJob_vol,
        'volume_mounts': cntryInfoAutoTrmnTrtJob_volMnt,
        'env_from':cntryInfoAutoTrmnTrtJob_env,
        'task_id':'cntryInfoAutoTrmnTrtJob',
        'image':'/icis/icis-oder-infocomm-batch:0.4.1.61',
        'arguments':["--job.name=cntryInfoAutoTrmnTrtJob", "reqDate=${YYYYMMDDHHMISS}", "procDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('db88c6972f394ed199b9c81be9fa180e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cntryInfoAutoTrmnTrtJob,
        Complete
    ]) 

    # authCheck >> cntryInfoAutoTrmnTrtJob >> Complete
    workflow








