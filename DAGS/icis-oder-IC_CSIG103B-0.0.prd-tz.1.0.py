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
                , WORKFLOW_NAME='IC_CSIG103B',WORKFLOW_ID='992c386a3e264834930737633516e273', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CSIG103B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 13, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('992c386a3e264834930737633516e273')

    csig103bJob_vol = []
    csig103bJob_volMnt = []
    csig103bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csig103bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csig103bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csig103bJob_env.extend([getICISConfigMap('icis-oder-infocomm-batch-mng-configmap'), getICISSecret('icis-oder-infocomm-batch-mng-secret'), getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISSecret('icis-oder-infocomm-batch-secret')])
    csig103bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csig103bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'cff2fb46fb134a04864a272981441e4e',
        'volumes': csig103bJob_vol,
        'volume_mounts': csig103bJob_volMnt,
        'env_from':csig103bJob_env,
        'task_id':'csig103bJob',
        'image':'/icis/icis-oder-infocomm-batch:0.7.1.7',
        'arguments':["--job.name=csig103bJob", "reqDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('992c386a3e264834930737633516e273')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csig103bJob,
        Complete
    ]) 

    # authCheck >> csig103bJob >> Complete
    workflow








