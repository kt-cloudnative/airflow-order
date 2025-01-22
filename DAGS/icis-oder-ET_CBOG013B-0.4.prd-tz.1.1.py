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
                , WORKFLOW_NAME='ET_CBOG013B',WORKFLOW_ID='454f425c6c4349cbb07a5d8674c1600e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOG013B-0.4.prd-tz.1.1'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 10, 14, 1, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('454f425c6c4349cbb07a5d8674c1600e')

    cbog013bJob_vol = []
    cbog013bJob_volMnt = []
    cbog013bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog013bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog013bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog013bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbog013bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog013bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b6821e3ac9b44485bd8a217aef3c9fe1',
        'volumes': cbog013bJob_vol,
        'volume_mounts': cbog013bJob_volMnt,
        'env_from':cbog013bJob_env,
        'task_id':'cbog013bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.7',
        'arguments':["--job.name=cbog013bJob", "inputDate=20241129", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('454f425c6c4349cbb07a5d8674c1600e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog013bJob,
        Complete
    ]) 

    # authCheck >> cbog013bJob >> Complete
    workflow








