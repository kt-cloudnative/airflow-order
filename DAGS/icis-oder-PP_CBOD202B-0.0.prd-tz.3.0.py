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
                , WORKFLOW_NAME='PP_CBOD202B',WORKFLOW_ID='e2b9dd90c5274aeea5fb34974df7161a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD202B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 7, 29, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e2b9dd90c5274aeea5fb34974df7161a')

    cbod202bJob_vol = []
    cbod202bJob_volMnt = []
    cbod202bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod202bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod202bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod202bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod202bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod202bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6a56b9a5791442a5a51b9e0eefa24453',
        'volumes': cbod202bJob_vol,
        'volume_mounts': cbod202bJob_volMnt,
        'env_from':cbod202bJob_env,
        'task_id':'cbod202bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.73',
        'arguments':["--job.name=cbod202bJob", "endTranDate=20240528", "fromTime=09", "toTime=19", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e2b9dd90c5274aeea5fb34974df7161a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod202bJob,
        Complete
    ]) 

    # authCheck >> cbod202bJob >> Complete
    workflow








