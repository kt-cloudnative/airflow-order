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
                , WORKFLOW_NAME='PP_CSNG952B',WORKFLOW_ID='c64174643d1b4426845c4842eb2f817f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG952B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 10, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c64174643d1b4426845c4842eb2f817f')

    csng952bJob_vol = []
    csng952bJob_volMnt = []
    csng952bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng952bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng952bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng952bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng952bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng952bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a9d79ee063ad466198d3071a0836ab58',
        'volumes': csng952bJob_vol,
        'volume_mounts': csng952bJob_volMnt,
        'env_from':csng952bJob_env,
        'task_id':'csng952bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=csng952bJob", "endTranDate=${YYYYMMDD}", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c64174643d1b4426845c4842eb2f817f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng952bJob,
        Complete
    ]) 

    # authCheck >> csng952bJob >> Complete
    workflow








