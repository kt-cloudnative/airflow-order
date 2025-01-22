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
                , WORKFLOW_NAME='PP_CSNG187B',WORKFLOW_ID='6c0f504681094482abac63e04b1501c6', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG187B-0.0.prd-tz.3.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 11, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6c0f504681094482abac63e04b1501c6')

    csng187bJob_vol = []
    csng187bJob_volMnt = []
    csng187bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng187bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng187bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng187bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng187bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng187bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b045ba4c247e4f0fba33f80b4d49bc0e',
        'volumes': csng187bJob_vol,
        'volume_mounts': csng187bJob_volMnt,
        'env_from':csng187bJob_env,
        'task_id':'csng187bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng187bJob", "pgmNm=csng187bJob" , "empNo=91156293"
, "endTranDate=202411"
, "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6c0f504681094482abac63e04b1501c6')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng187bJob,
        Complete
    ]) 

    # authCheck >> csng187bJob >> Complete
    workflow








