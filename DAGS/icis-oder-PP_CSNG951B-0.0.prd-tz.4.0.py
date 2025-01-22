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
                , WORKFLOW_NAME='PP_CSNG951B',WORKFLOW_ID='63196dbdd10b403ea06d73fbe193068b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG951B-0.0.prd-tz.4.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('63196dbdd10b403ea06d73fbe193068b')

    csng951bJob_vol = []
    csng951bJob_volMnt = []
    csng951bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng951bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng951bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng951bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng951bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng951bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8387f5e87e8b495fa0ba1361da8627d9',
        'volumes': csng951bJob_vol,
        'volume_mounts': csng951bJob_volMnt,
        'env_from':csng951bJob_env,
        'task_id':'csng951bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csng951bJob", "endTranDate=${YYYYMM}", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('63196dbdd10b403ea06d73fbe193068b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng951bJob,
        Complete
    ]) 

    # authCheck >> csng951bJob >> Complete
    workflow








