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
                , WORKFLOW_NAME='IN_CBOI146B',WORKFLOW_ID='4024d654e7f042ea991e3090ffffe646', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOI146B-0.4.prd-tz.2.0'
    ,'schedule_interval':'10 21 * * * '
    ,'start_date': datetime(2025, 1, 1, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4024d654e7f042ea991e3090ffffe646')

    cboi146bJob_vol = []
    cboi146bJob_volMnt = []
    cboi146bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cboi146bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cboi146bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboi146bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cboi146bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboi146bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0670baa0d8b642e3a6240390e839f87d',
        'volumes': cboi146bJob_vol,
        'volume_mounts': cboi146bJob_volMnt,
        'env_from':cboi146bJob_env,
        'task_id':'cboi146bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.4.1.65',
        'arguments':["--job.name=cboi146bJob", "endTranDate=${YYYYMMDD}", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4024d654e7f042ea991e3090ffffe646')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboi146bJob,
        Complete
    ]) 

    # authCheck >> cboi146bJob >> Complete
    workflow








